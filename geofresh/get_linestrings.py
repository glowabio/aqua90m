import json
import logging
import geomet.wkt
LOGGER = logging.getLogger(__name__)

def get_streamsegment_linestrings_geometry_coll(conn, subc_ids, basin_id, reg_id):

    ### Define query:
    '''
    Example query:
    SELECT ST_AsText(geom), subc_id
    FROM hydro.stream_segments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);

    Example result:
    st_astext                                                                                                                                                                                                                                                                                                                                                                                               |  subc_id
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------
    LINESTRING(9.917083333333334 54.70375,9.918750000000001 54.702083333333334,9.92125 54.702083333333334,9.922083333333335 54.70291666666667,9.924583333333334 54.70291666666667,9.925416666666667 54.702083333333334,9.927083333333334 54.702083333333334,9.927916666666668 54.70125,9.927083333333334 54.70041666666667,9.92875 54.69875,9.92875 54.697916666666664,9.929583333333333 54.69708333333333) | 506250459
    LINESTRING(9.919583333333334 54.69958333333334,9.922916666666667 54.69625,9.92375 54.69708333333333,9.924583333333334 54.69708333333333)                                                                                                                                                                                                                                                                | 506251015
    LINESTRING(9.924583333333334 54.69708333333333,9.925416666666667 54.69708333333333,9.926250000000001 54.697916666666664,9.927083333333334 54.697916666666664,9.927916666666668 54.69708333333333,9.929583333333333 54.69708333333333)                                                                                                                                                                   | 506251126
    LINESTRING(9.924583333333334 54.69291666666666,9.924583333333334 54.69375,9.92375 54.694583333333334,9.92375 54.69625,9.924583333333334 54.69708333333333)                                                                                                                                                                                                                                              | 506251712
    '''
    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712
    query = '''
    SELECT 
    ST_AsText(geom), subc_id
    FROM hydro.stream_segments
    WHERE subc_id IN ({relevant_ids})
    AND reg_id = {reg_id}
    AND basin_id = {basin_id}
    '''.format(relevant_ids = relevant_ids, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.debug('Iterating over the result rows, constructing GeoJSON...')
    linestrings_geojson = []
    while (True):
        row = cursor.fetchone()
        if row is None:
            break

        # Create GeoJSON geometry from each linestring:
        geometry = None
        if row[0] is not None:
            geometry = geomet.wkt.loads(row[0])
        else:
            # Geometry errors that happen when two segments flow into one outlet (Vanessa, 17 June 2024)
            # For example, subc_id 506469602, when routing from 507056424 to outlet -1294020
            LOGGER.error('Subcatchment %s has no geometry!' % row[1]) # for example: 506469602
            # Features with empty geometries:
            # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        linestrings_geojson.append(geometry)

    geometry_coll = {
        "type": "GeometryCollection",
        "geometries": linestrings_geojson
    }

    return geometry_coll


def get_streamsegment_linestrings_feature_coll(conn, subc_ids, basin_id, reg_id, add_subc_ids = False):

    ### Define query:
    '''
    Example query:
    SELECT ST_AsText(geom), subc_id, strahler
    FROM hydro.stream_segments WHERE subc_id IN (506250459, 506251015, 506251126, 506251712);

    Example result:
    st_astext                                                                                                                                                                                                                                                                                                                                                                                               |  subc_id
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------
    LINESTRING(9.917083333333334 54.70375,9.918750000000001 54.702083333333334,9.92125 54.702083333333334,9.922083333333335 54.70291666666667,9.924583333333334 54.70291666666667,9.925416666666667 54.702083333333334,9.927083333333334 54.702083333333334,9.927916666666668 54.70125,9.927083333333334 54.70041666666667,9.92875 54.69875,9.92875 54.697916666666664,9.929583333333333 54.69708333333333) | 506250459
    LINESTRING(9.919583333333334 54.69958333333334,9.922916666666667 54.69625,9.92375 54.69708333333333,9.924583333333334 54.69708333333333)                                                                                                                                                                                                                                                                | 506251015
    LINESTRING(9.924583333333334 54.69708333333333,9.925416666666667 54.69708333333333,9.926250000000001 54.697916666666664,9.927083333333334 54.697916666666664,9.927916666666668 54.69708333333333,9.929583333333333 54.69708333333333)                                                                                                                                                                   | 506251126
    LINESTRING(9.924583333333334 54.69291666666666,9.924583333333334 54.69375,9.92375 54.694583333333334,9.92375 54.69625,9.924583333333334 54.69708333333333)                                                                                                                                                                                                                                              | 506251712
    '''
    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712
    query = '''
    SELECT 
    ST_AsText(geom), subc_id, strahler
    FROM hydro.stream_segments
    WHERE subc_id IN ({relevant_ids})
    AND reg_id = {reg_id}
    AND basin_id = {basin_id}
    '''.format(relevant_ids = relevant_ids, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.debug('Iterating over the result rows, constructing GeoJSON...')
    features_geojson = []
    while (True):
        row = cursor.fetchone()
        if row is None:
            break

        # Create GeoJSON feature from each linestring:
        geometry = None
        if row[0] is not None:
            geometry = geomet.wkt.loads(row[0])
        else:
            # Geometry errors that happen when two segments flow into one outlet (Vanessa, 17 June 2024)
            # For example, subc_id 506469602, when routing from 507056424 to outlet -1294020
            LOGGER.error('Subcatchment %s has no linestring!' % row[1]) # for example: 506469602
            # Features with empty geometries:
            # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "subc_id": row[1],
                "strahler_order": row[2]
            }
        }
        features_geojson.append(feature)

    feature_coll = {
        "type": "FeatureCollection",
        "features": features_geojson,
        "basin_id": basin_id,
        "region_id": reg_id
    }

    if add_subc_ids:
        feature_coll["subc_ids"] = subc_ids

    return feature_coll


if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    from database_connection import connect_to_db
    from database_connection import get_connection_object

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

    print('\nSTART RUNNING FUNCTION: get_streamsegment_linestrings_geometry_coll')
    res = get_streamsegment_linestrings_geometry_coll(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)
    
    print('\nSTART RUNNING FUNCTION: get_streamsegment_linestrings_feature_coll')
    res = get_streamsegment_linestrings_feature_coll(conn, subc_ids, basin_id, reg_id, add_subc_ids=True)
    print('RESULT:\n%s' % res)
