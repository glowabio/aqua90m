import json
import logging
LOGGER = logging.getLogger(__name__)


def get_dijkstra_ids_one(conn, subc_id_start, subc_id_end, reg_id, basin_id):
    '''
    INPUT: subc_ids (start and end)
    OUTPUT: subc_ids (the entire path, incl. start and end)
    '''

    ### Define query:
    ### Construct SQL query:
    query = 'SELECT edge' # We are only interested in the subc_id of each line segment along the path.
    query += '''
    FROM pgr_dijkstra('
        SELECT
        subc_id AS id,
        subc_id AS source,
        target,
        length AS cost
        FROM hydro.stream_segments
        WHERE reg_id = {reg_id}
        AND basin_id = {basin_id}',
        {start_subc_id},
        {end_subc_id},
        directed := false);
    '''.format(reg_id = reg_id, basin_id = basin_id, subc_id_start = subc_id_start, subc_id_end = subc_id_end)
    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.debug('Iterating over the result rows...')
    all_ids = [subc_id_start] # Adding start segment, as it is not included in database return!
    while (True):
        row = cursor.fetchone()
        if row is None: break
        edge = row[0]

        if edge == -1: # pgr_dijkstra returns -1 as the last edge...
            pass

        else:
            all_ids.append(edge) # these are already integer!

    return all_ids


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
    basin_id = 1292547
    reg_id = 58

    #lon_start = 9.937520027160646
    #lat_start = 54.69422745526058
    subc_id_start = 506251713
    #lon_end = 9.9217
    #lat_end = 54.6917
    subc_id_end = 506251712

    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids')
    res = get_dijkstra_ids(conn, subc_id_start, subc_id_end, reg_id, basin_id)
    print('RESULT:\n%s' % res)
    
    # If you then want geometries for this, use functions
    # "get_streamsegment_linestrings_feature_coll" and
    # "get_streamsegment_linestrings_geometry_coll"
    # from module aqua90m.geofresh.get_linestrings:
    import get_linestrings

    # GeometryColl
    geom_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, res, basin_id, reg_id)
    print('\nRESULT (GeometryCollection):\n%s' % geom_coll)

    # Feature Coll
    feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(conn, res, basin_id, reg_id)
    print('\nRESULT (FeatureCollection/LineStrings):\n%s' % feature_coll)
