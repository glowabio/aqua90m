import psycopg2
import sys
import logging
import sshtunnel
import geomet.wkt
import os
import json
LOGGER = logging.getLogger(__name__)


###################################
### get results from SQL result ###
### Non-GeoJSON                 ###
###################################

def get_basin_id_reg_id(conn, subc_id):
    name = "get_basin_id_reg_id"
    LOGGER.debug("ENTERING: %s: subc_id=%s" % (name, subc_id))

    # Query info from database:
    query = """
    SELECT basin_id, reg_id
    FROM sub_catchments
    WHERE subc_id = {given_subc_id}
    """.format(given_subc_id = subc_id)
    query = query.replace("\n", " ")
    result_row = get_only_row(execute_query(conn, query), name)

    # Extract result from database return:
    if result_row is None:
        LOGGER.warning('No basin id and region id found for subc_id %s!' % subc_id)
        error_message = 'No basin id and region id found for subc_id %s!' % subc_id
        LOGGER.error(error_message)
        raise ValueError(error_message)
    else:
        basin_id = result_row[0]
        reg_id = result_row[1]
    LOGGER.debug("LEAVING: %s: subc_id=%s" % (name, subc_id))

    return basin_id, reg_id


def check_outside_europe(lon, lat):
    # TODO: Move somewhere else!
    outside_europe = False
    err_msg = None
    LOGGER.debug("CHECKING for in or outside Europe?!")

    if lat > 82:
        err_msg = 'Too far north to be part of Europe: %s' % lat
        outside_europe = True
    elif lat < 34:
        err_msg = 'Too far south to be part of Europe: %s' % lat
        outside_europe = True
    if lon > 70:
        err_msg = 'Too far east to be part of Europe: %s' % lon
        outside_europe = True
    elif lon < -32:
        err_msg = 'Too far west to be part of Europe: %s' % lon
        outside_europe = True

    if outside_europe:
        LOGGER.error(err_msg)
        raise ValueError(err_msg)


def get_reg_id(conn, lon, lat):
    name = "get_reg_id"
    LOGGER.debug("ENTERING: %s: lon=%s, lat=%s" % (name, lon, lat))

    check_outside_europe(lon, lat) # may raise ValueError!

    """
    Example query:
    SELECT reg_id FROM regional_units
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326), geom);

    Result:
     reg_id 
    --------
         58
    (1 row)
    """
    query = """
    SELECT reg_id
    FROM regional_units
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), geom)
    """.format(longitude = lon, latitude = lat)
    query = query.replace("\n", " ")
    result_row = get_only_row(execute_query(conn, query), name)
    
    if result_row is None:
        LOGGER.warning('No region id found for lon %s, lat %s! Is this in the ocean?' % (lon, lat)) # OCEAN CASE
        error_message = ('No result found for lon %s, lat %s! Is this in the ocean?' % (round(lon, 3), round(lat, 3))) # OCEAN CASE
        LOGGER.error(error_message)
        raise ValueError(error_message)

    else:
        reg_id = result_row[0]
    LOGGER.debug("LEAVING: %s: lon=%s, lat=%s: %s" % (name, lon, lat, reg_id))
    return reg_id


def get_subc_id_basin_id(conn, lon, lat, reg_id):
    name = "get_subc_id_basin_id"
    LOGGER.debug('ENTERING: %s for lon=%s, lat=%s' % (name, lon, lat))
    
    # Getting info from database:
    """
    Example query:
    SELECT sub.subc_id, sub.basin_id FROM sub_catchments sub
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326), sub.geom)
    AND sub.reg_id = 58;

    Result:
    subc_id    | basin_id
    -----------+----------
     506251252 |  1292547
    (1 row)
    """

    query = """
    SELECT
    subc_id,
    basin_id
    FROM sub_catchments
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), geom)
    AND reg_id = {reg_id}
    """.format(longitude = lon, latitude = lat, reg_id = reg_id)
    query = query.replace("\n", " ")

    result_row = get_only_row(execute_query(conn, query), name)
    
    if result_row is None:
        subc_id = None
        basin_id = None
        LOGGER.warning('No subc_id and basin_id. This should have been caught before. Does this latlon fall into the ocean?') # OCEAN CASE!
        error_message = ('No result (basin, subcatchment) found for lon %s, lat %s! Is this in the ocean?' % (lon, lat)) # OCEAN CASE
        LOGGER.error(error_message)
        raise ValueError(error_message)

    else:
        subc_id = result_row[0]
        basin_id = result_row[1]

    # Returning it...
    LOGGER.debug('LEAVING: %s for lon=%s, lat=%s --> subc_id %s, basin_id %s' % (name, lon, lat, subc_id, basin_id))
    return subc_id, basin_id 


###########################
### database connection ###
###########################

def get_max_upstream_catchments():

    global MAX_NUM_UPSTREAM_CATCHMENTS
    if MAX_NUM_UPSTREAM_CATCHMENTS is not None:
        LOGGER.debug("MAX_NUM_UPSTREAM_CATCHMENTS set already, returning it!")
        return MAX_NUM_UPSTREAM_CATCHMENTS

    # If it was not set yet, set it and return it:
    config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
    MAX_NUM_UPSTREAM_CATCHMENTS = 1000 # default...
    try:
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
            max_num = config["max_num_upstream_catchments"]
            MAX_NUM_UPSTREAM_CATCHMENTS = max_num
    except FileNotFoundError as e:
        LOGGER.info("Maximum upstream catchments not configured (config file not found), using default (%s)." % MAX_NUM_UPSTREAM_CATCHMENTS)
    except KeyError as e:
        LOGGER.info("Maximum upstream catchments not configured (config file does not contain item), using default (%s)." % MAX_NUM_UPSTREAM_CATCHMENTS)

    return MAX_NUM_UPSTREAM_CATCHMENTS


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
    LOGGER.debug("Starting SSH tunnel...")
    tunnel.start()
    LOGGER.debug("Starting SSH tunnel... done.")
    return tunnel


def connect_to_db(geofresh_server, db_port, database_name, database_username, database_password):
    # This blocks! Cannot run KeyboardInterrupt
    LOGGER.debug("Connecting to db...")
    conn = psycopg2.connect(
       database=database_name,
       user=database_username,
       password=database_password,
       host=geofresh_server,
       port= str(db_port)
    )
    LOGGER.debug("Connecting to db... done.")
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
    LOGGER.debug("Executing query...")
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor


def get_rows(cursor, num_rows, comment='unspecified function'):
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



if __name__ == "__main__":

    # This part is for testing the various functions, that"s why it is a bit makeshift.
    # In production, they would be called from the pygeoapi processes.
    #
    # source /home/mbuurman/work/pyg_geofresh/venv/bin/activate
    # python /home/mbuurman/work/pyg_geofresh/pygeoapi/pygeoapi/process/geofresh/py_query_db.py 9.931555 54.695070 dbpw pw
    #    where dbpw is the database passwort for postgresql, can be found in ~/.pgpass if you have access.
    #    where pw is your personal LDAP password for the ssh tunnel.

    if len(sys.argv) == 2:
        dbpw = sys.argv[1]
        mbpw = None
        use_tunnel = False

    elif len(sys.argv) == 3:
        dbpw = sys.argv[1]
        mbpw = sys.argv[2]
        use_tunnel = True
        print('Will try to make ssh tunnel with password "%s..."' % mbpw[0:1])

    else:
        print('Please provide a database password and (possibly an ssh tunnel password)...')
        sys.exit(1)

    verbose = True

    # Connection details:
    geofresh_server = "172.16.4.76"  # Hard-coded for testing
    geofresh_port = 5432             # Hard-coded for testing
    database_name = "geofresh_data"  # Hard-coded for testing
    database_username = "shiny_user" # Hard-coded for testing
    database_password = dbpw

    # Connection details for SSH tunneling:
    ssh_username = "mbuurman" # Hard-coded for testing
    ssh_password = mbpw
    localhost = "127.0.0.1"

    # Logging
    LOGGER = logging.getLogger()
    console = logging.StreamHandler()
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter("xxx %(name)-12s: %(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    LOGGER.addHandler(console)

    conn = get_connection_object(geofresh_server, geofresh_port,
        database_name, database_username, database_password,
        verbose=verbose, use_tunnel=use_tunnel,
        ssh_username=ssh_username, ssh_password=ssh_password)

    # Data for testing:
    # These coordinates are in Vantaanjoki, reg_id = 65, basin_id = 1274183, subc_id = 553495421
    #lat = 60.7631596
    #lon = 24.8919571
    # These coordinates are in Schlei, reg_id = 58, basin_id = 1292547, subc_id = 506251252
    lat = 54.695070
    lon = 9.931555

    # Run all queries:
    print("\n(1) reg_id: ")
    reg_id = get_reg_id(conn, lon, lat)
    print("\nRESULT REG_ID: %s" % reg_id)

    print("\n(2) subc_id, basin_id: ")
    subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
    print("\nRESULT BASIN_ID, SUBC_ID: %s, %s" % (basin_id, subc_id))
    
    

    ###################################
    ### dijkstra between two points ###
    ###################################

    #print("\n(9) DIJKSTRA ")
    # Falls into: 506 519 922, basin 1285755
    #lat2 = 53.695070
    #lon2 = 9.751555
    # Falls on boundary, error:
    #lon2 = 9.921666666666667 # falls on boundary!
    #lat2 = 54.69166666666666 # falls on boundary!
    # Falls into 506 251 713
    ##lon1 = 9.937520027160646
    ##lat1 = 54.69422745526058
    # Falls into: 506 251 712, basin 1292547
    ##lon2 = 9.9217
    ##lat2 = 54.6917
    ##subc_id_start, basin_id_dijkstra = get_subc_id_basin_id(conn, lon1, lat1, reg_id)
    ##subc_id_end, basin_id_end = get_subc_id_basin_id(conn, lon2, lat2, reg_id)
    ##print('Using start  subc_id: %s (%s)' % (subc_id_start, basin_id_dijkstra))
    ##print('Using target subc_id: %s (%s)' % (subc_id_end, basin_id_end))

    # Just the Ids:
    #segment_ids = get_dijkstra_ids(conn, subc_id_start, subc_id_end, reg_id, basin_id_dijkstra)
    #print('\nRESULT DIJKSTRA PATH segment_ids: %s\n' % segment_ids)
    
    # Feature Coll
    #feature_list = get_feature_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id1)
    #feature_coll = {"type": "FeatureCollection", "features": feature_list}
    #print('\nRESULT DIJKSTRA PATH TO SEA (FeatureCollection/LineStrings):\n%s' % feature_coll)
    
    # GeometryColl
    #dijkstra_path_list = get_simple_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id)
    #coll = {"type": "GeometryCollection", "geometries": dijkstra_path_list}
    #print('\nRESULT DIJKSTRA PATH TO SEA (GeometryCollection):\n%s' % coll)

    #######################
    ### dijkstra to sea ###
    #######################

    #print("\n(9b) DIJKSTRA TO SEA")
    # Falls into: 506 251 712, basin 1292547
    #lon1 = 9.937520027160646
    #lat1 = 54.69422745526058
    # Far away from sea, but yields no result at all!
    #lon1 = 10.599210072990063
    #lat1 = 51.31162492387419
    # bei Bremervoerde, leads to one non-geometry subcatchment, subc_id : 506469602
    #lat1 = 53.397626302268684
    #lon1 = 9.155709977606723
    # Not sure where this is:
    #lat1 = 52.76220968996532
    #lon1 = 11.558802055604199
    #subc_id_start, basin_id_dijkstra = get_subc_id_basin_id(conn, lon1, lat1, reg_id)
    #subc_id_end = -basin_id_dijkstra
    #print('Using start  subc_id: %s (%s)' % (subc_id_start, basin_id_dijkstra))
    #print('Using target subc_id: %s (%s)' % (subc_id_end, basin_id_dijkstra))
    
    # Just the Ids:
    #segment_ids = get_dijkstra_ids(conn, subc_id_start, subc_id_end, reg_id, basin_id_dijkstra)
    #print('\nRESULT DIJKSTRA PATH TO SEA segment_ids: %s\n' % segment_ids)
    
    # Feature Coll
    #coll = get_dijkstra_linestrings_feature_coll(conn, subc_id_start, subc_id_end, reg_id, basin_id_dijkstra, destination="sea")
    #feature_list = get_feature_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id1)
    #feature_coll = {"type": "FeatureCollection", "features": feature_list}
    #print('\nRESULT DIJKSTRA PATH TO SEA (FeatureCollection/LineStrings):\n%s' % feature_coll)
    
    # GeometryColl
    #dijkstra_path_list = get_simple_linestrings_for_subc_ids(conn, segment_ids, basin_id_dijkstra, reg_id)
    #coll = {"type": "GeometryCollection", "geometries": dijkstra_path_list}
    #print('\nRESULT DIJKSTRA PATH TO SEA (GeometryCollection):\n%s' % coll)

    # Finally:
    print("Closing connection...")
    conn.close()
    print("Done")
