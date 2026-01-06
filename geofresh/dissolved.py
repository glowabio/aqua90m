import json
import geomet.wkt
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

try:
    import aqua90m.geofresh.upstream_subcids as upstream_subcids
    import aqua90m.utils.exceptions as exc
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
        import pygeoapi.process.aqua90m.utils.exceptions as exc
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)


def get_dissolved_feature(conn, subc_ids, basin_id, reg_id, add_subc_ids = False):

    dissolved_simplegeom = get_dissolved_simplegeom(conn, subc_ids, basin_id, reg_id)
    # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    dissolved_feature = {
        "type": "Feature",
        "geometry": dissolved_simplegeom,
        "properties": {
            "basin_id": basin_id,
            "reg_id": reg_id
        }
    }

    if add_subc_ids:
        dissolved_feature["properties"]["subc_ids"] = subc_ids

    return dissolved_feature


def get_dissolved_simplegeom(conn, subc_ids, basin_id, reg_id):
    """
    Example result:
    {"type": "Polygon", "coordinates": [[[9.916666666666668, 54.7025], [9.913333333333334, 54.7025], [9.913333333333334, 54.705], [9.915000000000001, 54.705], [9.915833333333333, 54.705], [9.915833333333333, 54.70583333333333], [9.916666666666668, 54.70583333333333], [9.916666666666668, 54.705], [9.918333333333335, 54.705], [9.918333333333335, 54.704166666666666], [9.919166666666667, 54.704166666666666], [9.919166666666667, 54.70333333333333], [9.920833333333334, 54.70333333333333], [9.920833333333334, 54.704166666666666], [9.924166666666668, 54.704166666666666], [9.925, 54.704166666666666], [9.925, 54.705], [9.926666666666668, 54.705], [9.9275, 54.705], [9.9275, 54.70583333333333], [9.928333333333335, 54.70583333333333], [9.928333333333335, 54.70333333333333], [9.929166666666667, 54.70333333333333], [9.929166666666667, 54.7025], [9.931666666666667, 54.7025], [9.931666666666667, 54.7], [9.930833333333334, 54.7], [9.930833333333334, 54.69833333333333], [9.930000000000001, 54.69833333333333], [9.929166666666667, 54.69833333333333], [9.929166666666667, 54.6975], [9.929166666666667, 54.696666666666665], [9.928333333333335, 54.696666666666665], [9.928333333333335, 54.695], [9.9275, 54.695], [9.9275, 54.693333333333335], [9.928333333333335, 54.693333333333335], [9.928333333333335, 54.69166666666666], [9.9275, 54.69166666666666], [9.9275, 54.69083333333333], [9.926666666666668, 54.69083333333333], [9.926666666666668, 54.69], [9.925833333333333, 54.69], [9.925, 54.69], [9.925, 54.68833333333333], [9.922500000000001, 54.68833333333333], [9.922500000000001, 54.69083333333333], [9.921666666666667, 54.69083333333333], [9.921666666666667, 54.69166666666666], [9.919166666666667, 54.69166666666666], [9.919166666666667, 54.692499999999995], [9.918333333333335, 54.692499999999995], [9.918333333333335, 54.693333333333335], [9.9175, 54.693333333333335], [9.9175, 54.695], [9.918333333333335, 54.695], [9.918333333333335, 54.69833333333333], [9.9175, 54.69833333333333], [9.9175, 54.700833333333335], [9.9175, 54.70166666666667], [9.916666666666668, 54.70166666666667], [9.916666666666668, 54.7025]]]}
    """

    if len(subc_ids) == 0:
        LOGGER.info('No upstream ids, so cannot even query! Returning none.')
        LOGGER.warning('No upstream ids. Cannot get dissolved upstream catchment.')
        return None # Returning null geometry!
        # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
    
    # Get info from the database:
    """
    Example query:
    SELECT ST_AsText(ST_MemUnion(geom)) FROM sub_catchments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);

    Example result:
                                                         st_astext                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
    -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    POLYGON((9.916666666666668 54.7025,9.913333333333334 54.7025,9.913333333333334 54.705,9.915000000000001 54.705,9.915833333333333 54.705,9.915833333333333 54.70583333333333,9.916666666666668 54.70583333333333,9.916666666666668 54.705,9.918333333333335 54.705,9.918333333333335 54.704166666666666,9.919166666666667 54.704166666666666,9.919166666666667 54.70333333333333,9.920833333333334 54.70333333333333,9.920833333333334 54.704166666666666,9.924166666666668 54.704166666666666,9.925 54.704166666666666,9.925 54.705,9.926666666666668 54.705,9.9275 54.705,9.9275 54.70583333333333,9.928333333333335 54.70583333333333,9.928333333333335 54.70333333333333,9.929166666666667 54.70333333333333,9.929166666666667 54.7025,9.931666666666667 54.7025,9.931666666666667 54.7,9.930833333333334 54.7,9.930833333333334 54.69833333333333,9.930000000000001 54.69833333333333,9.929166666666667 54.69833333333333,9.929166666666667 54.6975,9.929166666666667 54.696666666666665,9.928333333333335 54.696666666666665,9.928333333333335 54.695,9.9275 54.695,9.9275 54.693333333333335,9.928333333333335 54.693333333333335,9.928333333333335 54.69166666666666,9.9275 54.69166666666666,9.9275 54.69083333333333,9.926666666666668 54.69083333333333,9.926666666666668 54.69,9.925833333333333 54.69,9.925 54.69,9.925 54.68833333333333,9.922500000000001 54.68833333333333,9.922500000000001 54.69083333333333,9.921666666666667 54.69083333333333,9.921666666666667 54.69166666666666,9.919166666666667 54.69166666666666,9.919166666666667 54.692499999999995,9.918333333333335 54.692499999999995,9.918333333333335 54.693333333333335,9.9175 54.693333333333335,9.9175 54.695,9.918333333333335 54.695,9.918333333333335 54.69833333333333,9.9175 54.69833333333333,9.9175 54.700833333333335,9.9175 54.70166666666667,9.916666666666668 54.70166666666667,9.916666666666668 54.7025))
    (1 row)
    """

    upstream_subcids.too_many_upstream_catchments(len(subc_ids), 'dissolved polygon')

    ### Define query:
    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712
    query = f'''
    SELECT ST_AsText(ST_MemUnion(geom))
    FROM sub_catchments
    WHERE subc_id IN ({relevant_ids})
        AND reg_id = {reg_id}
        AND basin_id = {basin_id}
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        err_msg = "Weird: No area (polygon) found in database."
        LOGGER.error(err_msg)
        raise exc.GeoFreshUnexpectedResultException(err_msg)

    # Assemble GeoJSON to return:
    dissolved_simplegeom = geomet.wkt.loads(row[0])
    return dissolved_simplegeom






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
        import aqua90m.geofresh.upstream_subcids as upstream_subcids
        import aqua90m.utils.exceptions as exc
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.geofresh.upstream_subcids as upstream_subcids
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
    LOGGER.log(logging.TRACE, 'Connecting to database... DONE.')

    ####################
    ### Run function ###
    ####################
    subc_ids = [506250459, 506251015, 506251126, 506251712]
    basin_id = 1292547
    reg_id = 58

    print('\nSTART RUNNING FUNCTION: get_dissolved_simplegeom')
    res = get_dissolved_simplegeom(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_dissolved_feature')
    res = get_dissolved_feature(conn, subc_ids, basin_id, reg_id, add_subc_ids = True)
    print('RESULT:\n%s' % res)

    #print('\nTEST CUSTOM EXCEPTION: get_dissolved_simplegeom...')
    #try:
        # Difficult to fake anything that causes the exception!
        #res = get_dissolved_simplegeom(...)
        #raise RuntimeError('Should not reach here!')
    #except exc.GeoFreshUnexpectedResultException as e:
        #print('RESULT: Proper exception, saying: %s' % e)
