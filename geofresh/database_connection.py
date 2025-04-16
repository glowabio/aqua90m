import psycopg2
import sys
import sshtunnel
import geomet.wkt
import os
import json
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

###########################
### database connection ###
###########################

def is_database_off(config_file_path = None):
    # TODO Test is_database_off!

    # Read value from config file, if available:
    if config_file_path is None:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
    try:
        DATABASE_OFF = False
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
            DATABASE_OFF = config["DATABASE_OFF"]
    except FileNotFoundError as e:
        LOGGER.info("Database-Emergency-Off not configured (config file not found), using default (%s)." % DATABASE_OFF)
    except KeyError as e:
        LOGGER.info("Database-Emergency-Off not configured (config file does not contain item), using default (%s)." % DATABASE_OFF)

    return DATABASE_OFF


def open_ssh_tunnel(ssh_host, ssh_username, ssh_password, remote_host, remote_port, verbose=False):
    """Open an SSH tunnel and connect using a username and password.
    
    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """
    LOGGER.info("Opening SSH tunnel...")
    #if verbose:
    #    #this works, but it is waaay to verbose!
    #    sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    #global tunnel
    tunnel = sshtunnel.SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address=(remote_host, remote_port)
    )
    LOGGER.log(logging.TRACE, "Starting SSH tunnel...")
    tunnel.start()
    LOGGER.log(logging.TRACE, "Starting SSH tunnel... done.")
    return tunnel


def get_connection_object_config(config):

    geofresh_server = config['geofresh_server']
    geofresh_port = config['geofresh_port']
    database_name = config['database_name']
    database_username = config['database_username']
    database_password = config['database_password']
    use_tunnel = config.get('use_tunnel')
    ssh_username = config.get('ssh_username')
    ssh_password = config.get('ssh_password')
    localhost = config.get('localhost')

    try:
        conn = get_connection_object(geofresh_server, geofresh_port,
            database_name, database_username, database_password,
            use_tunnel=use_tunnel, ssh_username=ssh_username, ssh_password=ssh_password)
    except sshtunnel.BaseSSHTunnelForwarderError as e1:
        LOGGER.error('SSH Tunnel Error: %s' % str(e1))
        raise e1

    return conn


def connect_to_db(geofresh_server, db_port, database_name, database_username, database_password):
    # This blocks! Cannot run KeyboardInterrupt
    LOGGER.log(logging.TRACE, "Connecting to db...")
    
    if is_database_off():
        LOGGER.error("Database was switched off via DATABASE_OFF in config.")
        raise RuntimeError("Compute service switched off for maintenance reasons. Sorry.")
    
    conn = psycopg2.connect(
       database=database_name,
       user=database_username,
       password=database_password,
       host=geofresh_server,
       port= str(db_port)
    )
    LOGGER.log(logging.TRACE, "Connecting to db... done.")
    return conn


def get_connection_object(geofresh_server, geofresh_port,
    database_name, database_username, database_password,
    verbose=False, use_tunnel=False, ssh_username=None, ssh_password=None):
    if use_tunnel:
        # See: https://practicaldatascience.co.uk/data-science/how-to-connect-to-mysql-via-an-ssh-tunnel-in-python
        ssh_host = geofresh_server
        remote_host = "127.0.0.1"
        remote_port = geofresh_port
        tunnel = open_ssh_tunnel(ssh_host, ssh_username, ssh_password, remote_host, remote_port, verbose)
        conn = connect_to_db(remote_host, tunnel.local_bind_port, database_name, database_username, database_password)
    else:
        conn = connect_to_db(geofresh_server, geofresh_port, database_name, database_username, database_password)
    return conn


def execute_query(conn, query):
    LOGGER.log(logging.TRACE, "Executing query...")
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor


def get_rows(cursor, num_rows, comment='unspecified function'):
    # TODO is this still used really?
    LOGGER.debug('get-rows (%s) for %s' % (num_rows, comment))
    i = 0
    return_rows = []
    while True:
        i += 1
        #LOGGER.debug("Fetching row %s..." % i)
        this_row = cursor.fetchone();
        if this_row is None and i == 1:
            LOGGER.error('Database returned no results at all (expected %s rows).' % num_rows)
            break
        elif this_row is None:
            break
        elif i <= num_rows:
            return_rows.append(this_row) # TODO: Do we need this? Just leave out the expected num_rows and let the "if this_row is None" do its job
        else:
            LOGGER.warning("Found more than %s rows in result! Row %s: %s" % (num_rows, i, this_row))
            LOGGER.info("WARNING: More than one row output! Will ignore row %s..." % i)

    return return_rows


def get_only_row(cursor, comment='unspecified function'):
    # TODO is this still used really?
    LOGGER.debug('get-only-row for function %s' % comment)
    i = 0
    return_row = None
    while True:
        i += 1
        #LOGGER.debug("Fetching row %s..." % i)
        this_row = cursor.fetchone()
        if this_row is None and i == 1:
            LOGGER.error('Database returned no results at all (expected one row).')
            break
        elif this_row is None:
            break
        elif i == 1:
            return_row = this_row
            LOGGER.debug("First and only row: %s" % str(this_row))
        else:
            # We are asking for one point, so the result should be just one row!
            # But if the point is exactly on a boundary, two can be returned! TODO how to deal with?
            # Example:
            # SELECT sub.subc_id, sub.basin_id FROM sub_catchments sub WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.921666666666667, 54.69166666666666),4326), sub.geom) AND sub.reg_id = 58;
            LOGGER.warning("Found more than 1 row in result! Row %s: %s" % (i, this_row))
            print("WARNING: More than one row output! Will ignore row %s..." % i)

    if return_row is None:
        LOGGER.error('Returning none, because we expected one row but got none (for %s).' % comment)

    return return_row


