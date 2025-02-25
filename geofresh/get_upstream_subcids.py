import json
import logging
import geomet.wkt
LOGGER = logging.getLogger(__name__)

# global variable:
MAX_NUM_UPSTREAM_CATCHMENTS = None

def get_max_upstream_catchments(config_file_path = None):

    global MAX_NUM_UPSTREAM_CATCHMENTS
    if MAX_NUM_UPSTREAM_CATCHMENTS is not None:
        LOGGER.debug("MAX_NUM_UPSTREAM_CATCHMENTS set already, returning it!")
        return MAX_NUM_UPSTREAM_CATCHMENTS

    # If it was not set yet, set it and return it:

    # Define default:
    MAX_NUM_UPSTREAM_CATCHMENTS = 1000

    # Read value from config file, if available:
    if config_file_path is None:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
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


def get_upstream_catchment_ids_incl_itself(conn, subc_id, basin_id, reg_id):

    ### Define query:
    # Getting info from database:
    """
    This one cuts the graph into connected components, by removing
    the segment-of-interest itself. As a result, its subcatchment
    is included in the result, and may have to be removed.

    Example query:
    SELECT 506251252, array_agg(node)::bigint[] AS nodes FROM pgr_connectedComponents('
        SELECT basin_id, subc_id AS id, subc_id AS source, target, length AS cost
        FROM hydro.stream_segments WHERE reg_id = 58 AND basin_id = 1292547 AND subc_id != 506251252
    ') WHERE component > 0 GROUP BY component;

    Result:
     ?column?  |                        nodes                        
    -----------+-----------------------------------------------------
     506251252 | {506250459,506251015,506251126,506251252,506251712}
    (1 row)
    """
    query = '''
    SELECT {subc_id}, array_agg(node)::bigint[] AS nodes 
    FROM pgr_connectedComponents('
        SELECT
        basin_id,
        subc_id AS id,
        subc_id AS source,
        target,
        length AS cost
        FROM hydro.stream_segments
        WHERE reg_id = {reg_id}
        AND basin_id = {basin_id}
        AND subc_id != {subc_id}
    ') WHERE component > 0 GROUP BY component;
    '''.format(subc_id = subc_id, reg_id = reg_id, basin_id = basin_id)
    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results:
    row = cursor.fetchone()

    # If no upstream catchments are returned:
    if row is None:
        LOGGER.info('No upstream catchment returned. Assuming this is a headwater. Returning just the local catchment itself.')
        return [subc_id]

    # Getting the info from the database:
    upstream_catchment_subcids = row[1]

    # Adding the subcatchment itself if it not returned:
    if not subc_id in upstream_catchment_subcids:
        upstream_catchment_subcids.append(subc_id)
        LOGGER.info('FYI: The database did not return the local subcatchment itself in the list of upstream subcatchments, so added it.')
    else:
        # This is what happens!
        LOGGER.debug('FYI: The database returned the local subcatchment itself in the list of upstream subcatchments, which is fine.')

    # Stop any computations with more than x upstream catchments!
    # TODO: Allow returning them, but then nothing else!
    max_num = get_max_upstream_catchments()
    if len(upstream_catchment_subcids) > max_num:
        LOGGER.warning('Limiting queries to %s upstream subcatchments' % max_num)
        LOGGER.info("LEAVING EMPTY: %s for subc_id (found %s upstream ids): %s" % (name, len(upstream_catchment_subcids), subc_id))
        #return []
        raise ValueError('Found %s subcatchments, but temporarily, calculations over %s subcatchments are not done.' % 
            (len(upstream_catchment_subcids), max_num))

    return upstream_catchment_subcids



if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    from py_query_db import connect_to_db
    from py_query_db import get_connection_object

    # Get config
    config_file_path = "./config.json"
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
        geofresh_server = config['geofresh_server']
        geofresh_port = config['geofresh_port']
        database_name = config['database_name']
        database_username = config['database_username']
        database_password = config['database_password']
        use_tunnel = config.get('use_tunnel')
        ssh_username = config.get('ssh_username')
        ssh_password = config.get('ssh_password')
        #localhost = config.get('localhost')

    # Connect to db:
    LOGGER.debug('Connecting to database...')
    conn = get_connection_object(
        geofresh_server, geofresh_port,
        database_name, database_username, database_password,
        verbose=verbose, use_tunnel=use_tunnel,
        ssh_username=ssh_username, ssh_password=ssh_password)
    #conn = connect_to_db(geofresh_server, geofresh_port, database_name,
    #database_username, database_password)
    LOGGER.debug('Connecting to database... DONE.')

    ####################
    ### Run function ###
    ####################
    #subc_ids = [506250459, 506251015, 506251126, 506251712]
    basin_id = 1292547
    reg_id = 58

    # For how many upstream catchments are we allowed to compute?
    print('\nSTART RUNNING FUNCTION: get_max_upstream_catchments')
    res = get_max_upstream_catchments(config_file_path = config_file_path)
    print('RESULT:\n%s' % res)

    one_subc_id = 506250459 # headwater
    one_subc_id = 506251015 # headwater
    one_subc_id = 506251712 # headwater
    print('\nSTART RUNNING FUNCTION: get_upstream_catchment_ids_incl_itself')
    res = get_upstream_catchment_ids_incl_itself(conn, one_subc_id, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    one_subc_id = 506251126 # returns 3 subc_ids
    print('\nSTART RUNNING FUNCTION: get_upstream_catchment_ids_incl_itself')
    res = get_upstream_catchment_ids_incl_itself(conn, one_subc_id, basin_id, reg_id)
    print('RESULT:\n%s' % res)
