import json
import geomet.wkt
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

try:
    # If the package is installed in local python PATH:
    #import aqua90m.geofresh.upstream_subcids as upstream_subcids
    # For some reason, this fixed it, when this module was called from routing,
    # so it was not __main__, and this aqua90m was not added to local python PATH...
    import upstream_subcids as upstream_subcids
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)


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

    upstream_subcids.too_many_upstream_catchments(len(subc_ids), 'individual stream segments')


    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712
    query = f'''
    SELECT 
        ST_AsText(geom), subc_id
    FROM hydro.stream_segments
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
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
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
            LOGGER.error(f'Subcatchment {row[1]} has no geometry!') # for example: 506469602
            # Features with empty geometries:
            # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        linestrings_geojson.append(geometry)

    geometry_coll = {
        "type": "GeometryCollection",
        "geometries": linestrings_geojson
    }

    return geometry_coll


def get_streamsegment_linestrings_feature_coll(conn, subc_ids, basin_id, reg_id):

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

    upstream_subcids.too_many_upstream_catchments(len(subc_ids), 'individual stream segments')


    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712
    query = f'''
    SELECT 
        ST_AsText(geom), subc_id, strahler
    FROM hydro.stream_segments
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
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
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

    return feature_coll


def get_accum_length_by_strahler(conn, subc_ids, basin_id, reg_id):
    # TODO: Maybe just add this to one of the above...? Or might users want
    # the cumulative length without requesting the geometries?

    # Define query:
    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    query = f'''
    SELECT length, strahler
    FROM stream_segments
    WHERE subc_id IN ({relevant_ids})
        AND reg_id = {reg_id}
        AND basin_id = {basin_id}
    '''

    # Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    # Iterate over result rows
    cum_length = 0
    length_by_strahler = {}
    while (True):
        row = cursor.fetchone()
        if row is None:
            break

        length = row[0]
        strahler = row[1]
        cum_length += length
        LOGGER.log(logging.TRACE, 'Length of this segment: %s, cum %s, strahler %s' % (length, cum_length, strahler))
        if str(strahler) in length_by_strahler:
            length_by_strahler[str(strahler)] += length
        else:
            length_by_strahler[str(strahler)] = length

    length_by_strahler["all_strahler_orders"] = cum_length
    LOGGER.log(logging.TRACE, 'Returning dict: %s' % length_by_strahler)
    return length_by_strahler


def get_accum_length(conn, subc_ids, basin_id, reg_id):

    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    query = f'''
    SELECT length
    FROM stream_segments
    WHERE subc_id IN ({relevant_ids})
        AND reg_id = {reg_id}
        AND basin_id = {basin_id}
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Iterate over results
    cum_length = 0
    while (True):
        row = cursor.fetchone()
        if row is None:
            break

        cum_length += row[0]
        LOGGER.log(logging.TRACE, 'Length of this segment: %s, cum: %s' % (row[0], cum_length))

    return cum_length


def get_streamsegment_linestrings_geometry_coll_by_basin(conn, basin_id, reg_id, strahler_min=0):

    query = f'''
    SELECT 
        ST_AsText(geom), subc_id, target, length, strahler
    FROM hydro.stream_segments
    WHERE basin_id = {basin_id}
        AND reg_id = {reg_id}
        AND strahler >= {strahler_min}
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
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
            LOGGER.error(f'Subcatchment {row[1]} has no geometry!') # for example: 506469602
            # Features with empty geometries:
            # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        linestrings_geojson.append(geometry)

    geometry_coll = {
        "type": "GeometryCollection",
        "geometries": linestrings_geojson
    }

    return geometry_coll


def get_streamsegment_linestrings_feature_coll_by_basin(conn, basin_id, reg_id, strahler_min=0):

    ### Define query:
    '''
    Example query:

    Example result:
    '''


    query = f'''
    SELECT
        ST_AsText(geom), subc_id, target, length, strahler
    FROM hydro.stream_segments
    WHERE basin_id = {basin_id}
        AND reg_id = {reg_id}
        AND strahler >= {strahler_min}
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
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
                "target": row[2],
                "length": row[3],
                "strahler_order": row[4]
            }
        }
        features_geojson.append(feature)

    feature_coll = {
        "type": "FeatureCollection",
        "features": features_geojson,
        "basin_id": basin_id,
        "region_id": reg_id,
        "number_stream_segments": len(features_geojson)
    }

    return feature_coll


if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    #logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.TRACE, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    # Local imports:
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
    LOGGER.log(logging.TRACE, 'Connecting to database... DONE.')

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
    res = get_streamsegment_linestrings_feature_coll(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_accum_length_by_strahler')
    res = get_accum_length_by_strahler(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_accum_length_by_strahler')
    res = get_accum_length(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)
