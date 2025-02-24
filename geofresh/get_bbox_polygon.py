import json
import geomet.wkt
import logging
LOGGER = logging.getLogger(__name__)


def get_bbox_polygon(conn, subc_ids, basin_id, reg_id):
    """
    Returns GeoJSON Geometry (can be None / null)!
    Example result:
    {
      "type": "Polygon",
      "coordinates": [[
        [9.913333333333334, 54.68833333333333],
        [9.913333333333334, 54.70583333333333],
        [9.931666666666667, 54.70583333333333],
        [9.931666666666667, 54.68833333333333],
        [9.913333333333334, 54.68833333333333]
      ]]
    }
    """

    if len(subc_ids) == 0:
        LOGGER.warning('No subc_ids. Cannot get bbox.')
        return None # returning null geometry
        # A geometry can be None/null, which is the valid value for
        # unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2


    ### Define query:
    """
    Example query:
    SELECT ST_AsText(ST_Extent(geom)) FROM sub_catchments WHERE reg_id = 58 AND basin_id = 1292547 AND subc_id IN (506250459, 506251015, 506251126, 506251712);

    These queries return the same result:
    geofresh_data=> SELECT ST_AsText(ST_Extent(geom)) as bbox FROM sub_catchments WHERE reg_id = 58 AND basin_id = 1292547 AND subc_id IN (506250459, 506251015, 506251126, 506251712) GROUP BY reg_id;
    geofresh_data=> SELECT ST_AsText(ST_Extent(geom)) as bbox FROM sub_catchments WHERE reg_id = 58 AND basin_id = 1292547 AND subc_id IN (506250459, 506251015, 506251126, 506251712);
    geofresh_data=> SELECT ST_AsText(ST_Extent(geom)) as bbox FROM sub_catchments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    POLYGON((9.913333333333334 54.68833333333333,9.913333333333334 54.70583333333333,9.931666666666667 54.70583333333333,9.931666666666667 54.68833333333333,9.913333333333334 54.68833333333333))
    (1 row)
    """
    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712
    query = """
    SELECT ST_AsText(ST_Extent(geom))
    FROM sub_catchments
    WHERE subc_id IN ({relevant_ids})
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    """.format(relevant_ids = relevant_ids, basin_id = basin_id, reg_id = reg_id)

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:

    # Get row from database:
    result_row = cursor.fetchone();
    bbox_wkt = result_row[0]

    # Assemble GeoJSON to return:
    bbox_geojson = geomet.wkt.loads(bbox_wkt)
    return bbox_geojson


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
    subc_ids = [506250459, 506251015, 506251126, 506251712]
    basin_id = 1292547
    reg_id = 58
    print('\nSTART RUNNING FUNCTION:')
    res = get_bbox_polygon(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)
