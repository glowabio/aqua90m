import json
import geomet.wkt
import logging
LOGGER = logging.getLogger(__name__)

# TODO: FUTURE: If we ever snap to stream segments outside of the immediate subcatchment,
# need to adapt some stuff in this process...

def get_snapped_point_geometry_coll(conn, lon, lat, subc_id, basin_id, reg_id):
    return _get_snapped_point_plus(conn, lon, lat, subc_id, basin_id, reg_id, make_feature = False)

def get_snapped_point_feature_coll(conn, lon, lat, subc_id, basin_id, reg_id):
    return _get_snapped_point_plus(conn, lon, lat, subc_id, basin_id, reg_id, make_feature = True)

def _get_snapped_point_plus(conn, lon, lat, subc_id, basin_id, reg_id, make_feature = False):
    ### Define query:
    """
    Example query:
    SELECT
    ST_AsText(ST_LineInterpolatePoint(geom, ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint(9.931555, 54.695070), 4326)))),
    ST_AsText(geom),
    strahler
    FROM hydro.stream_segments WHERE subc_id = 506251252;

    Example result:
    st_astext                |                                                                                    st_astext                                                                                    | strahler 
    -------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------
    POINT(9.931555 54.69625) | LINESTRING(9.929583333333333 54.69708333333333,9.930416666666668 54.69625,9.932083333333335 54.69625,9.933750000000002 54.694583333333334,9.934583333333334 54.694583333333334) |        2
    (1 row)

    """
    query = '''
    SELECT
    ST_AsText(ST_LineInterpolatePoint(geom, ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326)))),
    ST_AsText(geom),
    strahler
    FROM hydro.stream_segments
    WHERE subc_id = {subc_id}
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    '''.format(subc_id = subc_id, longitude = lon, latitude = lat, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")
    LOGGER.debug("QUUUUERY: %s" % query)

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        LOGGER.warning("Received result_row None for point: lon=%s, lat=%s (subc_id %s). This is weird. Any point should be snappable, right?" % (lon, lat, subc_id))
        err_msg = "Weird: Could not snap point lon=%s, lat=%s" % (lon, lat) 
        LOGGER.error(err_msg)
        raise ValueError(err_msg)
        # Or return features with empty geometries:
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    # Assemble GeoJSON to return:
    snappedpoint_simplegeom = geomet.wkt.loads(row[0])
    streamsegment_simplegeom = geomet.wkt.loads(row[1])
    strahler = row[2]

    # Extract snapped coordinates:
    snap_lon = snappedpoint_simplegeom["coordinates"][0]
    snap_lat = snappedpoint_simplegeom["coordinates"][1]

    # Make connecting line:
    connecting_line_simplegeom = {
        "type": "LineString",
        "coordinates":[[lon, lat], [snap_lon, snap_lat]]
    }

    if not make_feature:
        geometry_coll = {
            "type": "GeometryCollection",
            "geometries": [snappedpoint_simplegeom, streamsegment_simplegeom, connecting_line_simplegeom]
        }
        return geometry_coll

    if make_feature:
        # TODO: Rethink. Redundant: Currently, all features get the same properties.
        # We could add them to the FeatureCollection, but then they are not
        # part of the official GeoJSON, as FeatureCollections do not have
        # "properties".

        snappedpoint_feature = {
            "type": "Feature",
            "geometry": snappedpoint_simplegeom,
            "properties": {
                "strahler": strahler,
                "subc_id": subc_id,
                "basin_id": basin_id,
                "reg_id": reg_id,
                "lon_original": lon,
                "lat_original": lat,
            }
        }

        streamsegment_feature = {
            "type": "Feature",
            "geometry": streamsegment_simplegeom,
            "properties": {
                "strahler": strahler,
                "subc_id": subc_id,
                "basin_id": basin_id,
                "reg_id": reg_id,
                "lon_original": lon,
                "lat_original": lat,
            }
        }

        connecting_line_feature = {
            "type": "Feature",
            "geometry": connecting_line_simplegeom,
            "properties": {
                "strahler": strahler,
                "subc_id": subc_id,
                "basin_id": basin_id,
                "reg_id": reg_id,
                "lon_original": lon,
                "lat_original": lat,
                "description": "connecting line"
            }
        }

        feature_coll = {
            "type": "FeatureCollection",
            "features": [snappedpoint_feature, streamsegment_feature, connecting_line_feature]
        }
        return feature_coll

    return snappedpoint_feature

def get_snapped_point_feature(conn, lon, lat, subc_id, basin_id, reg_id):

    ### Define query:
    """
    SELECT seg.strahler,
    ST_AsText(ST_LineInterpolatePoint(geom, ST_LineLocatePoint(seg.geom, ST_SetSRID(ST_MakePoint(9.931555, 54.695070), 4326)))),
    ST_AsText(geom)
    FROM hydro.stream_segments seg WHERE subc_id = 506251252;

    Result:
     strahler |        st_astext         |                                                                                    st_astext                                                                                    
    ----------+--------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            2 | POINT(9.931555 54.69625) | LINESTRING(9.929583333333333 54.69708333333333,9.930416666666668 54.69625,9.932083333333335 54.69625,9.933750000000002 54.694583333333334,9.934583333333334 54.694583333333334)
    (1 row)
    """
    query = '''
    SELECT
    ST_AsText(ST_LineInterpolatePoint(geom, ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326)))),
    geom,
    strahler
    FROM hydro.stream_segments
    WHERE subc_id = {subc_id}
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    '''.format(subc_id = subc_id, longitude = lon, latitude = lat, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        LOGGER.warning("Received result_row None for point: lon=%s, lat=%s (subc_id %s). This is weird. Any point should be snappable, right?" % (lon, lat, subc_id))
        err_msg = "Weird: Could not snap point lon=%s, lat=%s" % (lon, lat) 
        LOGGER.error(err_msg)
        raise ValueError(err_msg)
        # Or return features with empty geometries:
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    # Assemble GeoJSON to return:
    snappedpoint_simplegeom = geomet.wkt.loads(row[0])
    #streamsegment_simplegeom = geomet.wkt.loads(row[1])
    strahler = row[2]

    snappedpoint_feature = {
        "type": "Feature",
        "geometry": snappedpoint_simplegeom,
        "properties": {
            "strahler": strahler,
            "subc_id": subc_id,
            "basin_id": basin_id,
            "reg_id": reg_id,
            "lon_original": lon,
            "lat_original": lat,
        }
    }

    return snappedpoint_feature

#def get_snapped_point_simple(conn, lon, lat, subc_id, basin_id, reg_id):
def get_snapped_point_simplegeom(conn, lon, lat, subc_id, basin_id, reg_id):
    """
    Example result:
    2, {"type": "Point", "coordinates": [9.931555, 54.69625]}, {"type": "LineString", "coordinates": [[9.929583333333333, 54.69708333333333], [9.930416666666668, 54.69625], [9.932083333333335, 54.69625], [9.933750000000002, 54.694583333333334], [9.934583333333334, 54.694583333333334]]}

    """
    
    ### Define query:
    """
    SELECT seg.strahler,
    ST_AsText(ST_LineInterpolatePoint(seg.geom, ST_LineLocatePoint(seg.geom, ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326)))),
    ST_AsText(seg.geom)
    FROM hydro.stream_segments seg WHERE seg.subc_id = 506251252;

    Result:
     strahler |        st_astext         |                                                                                    st_astext                                                                                    
    ----------+--------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            2 | POINT(9.931555 54.69625) | LINESTRING(9.929583333333333 54.69708333333333,9.930416666666668 54.69625,9.932083333333335 54.69625,9.933750000000002 54.694583333333334,9.934583333333334 54.694583333333334)
    (1 row)
    """
    query = '''
    SELECT
    ST_AsText(ST_LineInterpolatePoint(geom, ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326))))
    FROM hydro.stream_segments
    WHERE subc_id = {subc_id}
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    '''.format(subc_id = subc_id, longitude = lon, latitude = lat, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        LOGGER.warning("Received result_row None for point: lon=%s, lat=%s (subc_id %s). This is weird. Any point should be snappable, right?" % (lon, lat, subc_id))
        err_msg = "Weird: Could not snap point lon=%s, lat=%s" % (lon, lat) 
        LOGGER.error(err_msg)
        raise ValueError(err_msg)
        # Or return features with empty geometries:
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    # Assemble GeoJSON to return:
    snappedpoint_simplegeom = geomet.wkt.loads(row[0])
    return snappedpoint_simplegeom



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
    lon = 9.931555
    lat = 54.695070
    subc_id = 506251252
    basin_id = 1292547
    reg_id = 58

    print('\nSTART RUNNING FUNCTION: get_snapped_point_simplegeom')
    res = get_snapped_point_simplegeom(conn, lon, lat, subc_id, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_feature')
    res = get_snapped_point_feature(conn, lon, lat, subc_id, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_feature_coll')
    res = get_snapped_point_feature_coll(conn, lon, lat, subc_id, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_feature_coll')
    res = get_snapped_point_feature_coll(conn, lon, lat, subc_id, basin_id, reg_id)
    print('RESULT:\n%s' % res)
