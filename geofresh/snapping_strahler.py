import json
import uuid
import time
import geomet.wkt
import pandas as pd
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

try:
    # If the package is installed in local python PATH:
    import aqua90m.utils.geojson_helpers as geojson_helpers
    import aqua90m.utils.exceptions as exc
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
        import pygeoapi.process.aqua90m.utils.exceptions as exc
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)

###########################
### One point at a time ###
###########################

def get_snapped_point_geometry_coll(conn, lon, lat, strahler, basin_id, reg_id):
    # Just a wrapper!
    # INPUT: lon, lat
    # OUTPUT: GeometryCollection (point, stream segment, connecting line)
    return _get_snapped_point_plus(conn, lon, lat, strahler, basin_id, reg_id, make_feature = False)


def get_snapped_point_feature_coll(conn, lon, lat, strahler, basin_id, reg_id):
    # Just a wrapper!
    # INPUT: subc_id
    # OUTPUT: FeatureCollection (point, stream segment, connecting line)
    return _get_snapped_point_plus(conn, lon, lat, strahler, basin_id, reg_id, make_feature = True)


def _get_snapped_point_plus(conn, lon, lat, strahler, basin_id, reg_id, make_feature = False):
    # INPUT: lon, lat, strahler
    # OUTPUT: FeatureCollection or GeometryCollection (point, stream segment, connecting line)
    LOGGER.debug('Snapping point lon %s, lat %s to closest strahler %s (in basin %s, region %s)...' % (lon, lat, strahler, basin_id, reg_id))

    ### Define query:
    """
    Example query:
    SELECT 
        ST_AsGeoJSON(ST_LineInterpolatePoint(
            closest.geom,
            ST_LineLocatePoint(closest.geom, ST_SetSRID(ST_MakePoint(9.931555, 54.695070), 4326))
        )),
    closest.subc_id,
    closest.strahler
    FROM (
        SELECT 
            seg.subc_id,
            seg.strahler,
            seg.geom AS geom,
            seg.geom <-> ST_SetSRID(ST_MakePoint(9.931555, 54.695070), 4326)::geometry AS dist
        FROM hydro.stream_segments seg
        WHERE seg.strahler >= 3
        ORDER BY dist
        LIMIT 1
    ) AS closest;

    Example result:
                          st_asgeojson                        |  subc_id  | strahler 
    ----------------------------------------------------------+-----------+----------
    {"type":"Point","coordinates":[9.939583333,54.695416667]} | 506251482 |        3
    {"type":"Point","coordinates":[9.940416667,54.694583333]} | 506251714 |        3
    {"type":"Point","coordinates":[9.940416667,54.690416667]} | 506252174 |        3

    """
    query = '''
    SELECT 
        ST_AsText(ST_LineInterpolatePoint(
            closest.geom,
            ST_LineLocatePoint(closest.geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326))
        )),
        ST_AsText(closest.geom),
        closest.strahler,
        closest.subc_id
    FROM (
        SELECT 
            seg.subc_id,
            seg.strahler,
            seg.geom AS geom,
            seg.geom <-> ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326)::geometry AS dist
        FROM hydro.stream_segments seg
        WHERE seg.strahler >= {strahler}
        ORDER BY dist
        LIMIT 1
    ) AS closest;
    '''.format(strahler = strahler, longitude = lon, latitude = lat, basin_id = basin_id, reg_id = reg_id)
    query = query.replace("\n", " ")
    LOGGER.log(logging.TRACE, "SQL query: %s" % query)

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        LOGGER.warning("Received result_row None for point: lon=%s, lat=%s. This is weird. Any point should be snappable, right?" % (lon, lat))
        err_msg = "Weird: Could not snap point lon=%s, lat=%s" % (lon, lat) 
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)
        # Or return features with empty geometries:
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    LOGGER.debug("ROW: %s" % str(row))
    # Assemble GeoJSON to return:
    snappedpoint_simplegeom = geomet.wkt.loads(row[0])
    streamsegment_simplegeom = geomet.wkt.loads(row[1])
    strahler = row[2]
    subc_id = row[3]

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
            "features": [snappedpoint_feature, streamsegment_feature, connecting_line_feature],
            "subc_id_after_snapping": subc_id
        }
        # TODO: subc_after_snapping is added several times!
        return feature_coll

    return None # GeoJSON has been returned before!



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
        import aqua90m.utils.geojson_helpers as geojson_helpers
        import aqua90m.utils.exceptions as exc
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.utils.geojson_helpers as geojson_helpers
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

    ######################################
    ### Run function for single points ###
    ######################################

    lon = 9.931555
    lat = 54.695070
    strahler = 1
    #subc_id = 506251252
    basin_id = 1292547
    reg_id = 58

    print('\nSTART RUNNING FUNCTION: get_snapped_point_simplegeom')
    start = time.time()
    res = get_snapped_point_geometry_coll(conn, lon, lat, strahler, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_feature')
    start = time.time()
    res = get_snapped_point_feature_coll(conn, lon, lat, strahler, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)
