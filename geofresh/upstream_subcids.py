import json
import os
import geomet.wkt
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

try:
    # If the package is installed in local python PATH:
    import aqua90m.utils.exceptions as exc
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.exceptions as exc
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)

# global variable:
MAX_NUM_UPSTREAM_CATCHMENTS = None


def get_max_upstream_catchments(config_file_path = None):

    global MAX_NUM_UPSTREAM_CATCHMENTS
    if MAX_NUM_UPSTREAM_CATCHMENTS is not None:
        LOGGER.log(logging.TRACE, "MAX_NUM_UPSTREAM_CATCHMENTS set already, returning it!")
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


def too_many_upstream_catchments(num, func_name, config_file_path = None, fake = None):
    max_num = get_max_upstream_catchments(config_file_path)
    if fake is not None:
        max_num = fake
    if num > max_num:
        err_msg = "Exceeded limit of catchments (%s, limit = %s) for which we are allowed to request %s" % (num, max_num, func_name)
        LOGGER.error(err_msg)
        raise exc.GeoFreshTooManySubcatchments(err_msg)


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
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

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
    # Instead: We allow returning them, but then nothing else! Preventing to do
    # anything else then has to be done in the functions where this anything
    # else would happen with the upstream catchment ids as input!
    '''
    max_num = get_max_upstream_catchments()
    if len(upstream_catchment_subcids) > max_num:
        LOGGER.warning('Limiting queries to %s upstream subcatchments' % max_num)
        raise exc.GeoFreshTooManySubcatchments('Found %s subcatchments, but temporarily, calculations over %s subcatchments are not done.' % 
            (len(upstream_catchment_subcids), max_num))
    '''

    return upstream_catchment_subcids



if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    from database_connection import connect_to_db
    from database_connection import get_connection_object

    try:
        # If the package is properly installed, thus it is findable by python on PATH:
        import aqua90m.utils.exceptions as exc
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.utils.exceptions as exc

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

    # Test throwing custom exceptions:
    print('\nTEST CUSTOM EXCEPTION: too_many_upstream_catchments')
    try:
        res = too_many_upstream_catchments(20, 'dummy', config_file_path = None, fake = 2)
        raise RuntimeError('Should not reach here!')
    except exc.GeoFreshTooManySubcatchments as e:
        print('RESULT: Proper exception, saying: %s' % e)

    print('\nSTART RUNNING FUNCTION: get_upstream_catchment_ids_incl_itself (for three headwaters)')
    res = get_upstream_catchment_ids_incl_itself(conn, 506250459, basin_id, reg_id)
    print('RESULT:\n%s' % res)
    res = get_upstream_catchment_ids_incl_itself(conn, 506251015, basin_id, reg_id)
    print('RESULT:\n%s' % res)
    res = get_upstream_catchment_ids_incl_itself(conn, 506251712, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_upstream_catchment_ids_incl_itself (returns three)')
    res = get_upstream_catchment_ids_incl_itself(conn, 506251126, basin_id, reg_id)
    print('RESULT:\n%s' % res)
