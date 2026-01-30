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
    import aqua90m.geofresh.temp_table_for_queries as temp_table_for_queries
    from aqua90m.geofresh.temp_table_for_queries import log_query_time as log_query_time
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
        import pygeoapi.process.aqua90m.utils.exceptions as exc
        import pygeoapi.process.aqua90m.geofresh.temp_table_for_queries as temp_table_for_queries
        from pygeoapi.process.aqua90m.geofresh.temp_table_for_queries import log_query_time as log_query_time
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)

# TODO: FUTURE: If we ever snap to stream segments outside of the immediate subcatchment,
# need to adapt some stuff in this process...


###########################
### One point at a time ###
###########################

# Just a wrapper!
def get_snapped_point_geometry_coll(conn, lon, lat, subc_id, basin_id, reg_id):
    # INPUT: subc_id
    # OUTPUT: GeometryCollection (point, stream segment, connecting line)
    return _get_snapped_point_plus(conn, lon, lat, subc_id, basin_id, reg_id, make_feature = False)


# Just a wrapper!
def get_snapped_point_feature_coll(conn, lon, lat, subc_id, basin_id, reg_id):
    # INPUT: subc_id
    # OUTPUT: FeatureCollection (point, stream segment, connecting line)
    return _get_snapped_point_plus(conn, lon, lat, subc_id, basin_id, reg_id, make_feature = True)


def _get_snapped_point_plus(conn, lon, lat, subc_id, basin_id, reg_id, make_feature = False):
    # INPUT: subc_id
    # OUTPUT: FeatureCollection or GeometryCollection (point, stream segment, connecting line)
    LOGGER.debug('Snapping point in subcatchment %s, basin %s, region %s ("mit alles")' % (subc_id, basin_id, reg_id))

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
    query = f'''
    SELECT
        ST_AsText(ST_LineInterpolatePoint(
            geom,
            ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))
        )),
        ST_AsText(geom),
        strahler
    FROM hydro.stream_segments
    WHERE
        subc_id = {subc_id}
        AND basin_id = {basin_id}
        AND reg_id = {reg_id}
    '''

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    cursor = conn.cursor()
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'basic snapping (one point)')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        LOGGER.warning("Received result_row None for point: lon=%s, lat=%s (subc_id %s). This is weird. Any point should be snappable, right?" % (lon, lat, subc_id))
        err_msg = "Weird: Could not snap point lon=%s, lat=%s" % (lon, lat) 
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)
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

    return None # GeoJSON has been returned before!


def get_snapped_point_feature(conn, lon, lat, subc_id, basin_id, reg_id):
    # INPUT: subc_id
    # OUTPUT: Feature (Point)

    LOGGER.debug('Snapping point in subcatchment %s, basin %s, region %s (just the Feature)' % (subc_id, basin_id, reg_id))

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
    query = f'''
    SELECT
        ST_AsText(ST_LineInterpolatePoint(
            geom,
            ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint({lon}, {lat}),4326))
        )),
        geom,
        strahler
    FROM hydro.stream_segments
    WHERE
        subc_id = {subc_id}
        AND basin_id = {basin_id}
        AND reg_id = {reg_id}
    '''

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    cursor = conn.cursor()
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'basic snapping (one point)')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        LOGGER.warning("Received result_row None for point: lon={lon}, lat={lat} (subc_id {subc_id}). This is weird. Any point should be snappable, right?")
        err_msg = f"Weird: Could not snap point lon={lon}, lat={lat}"
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)
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


def get_snapped_point_simplegeom(conn, lon, lat, subc_id, basin_id, reg_id):
    # INPUT: subc_id
    # OUTPUT: Geometry (Point)

    LOGGER.debug('Snapping point in subcatchment %s, basin %s, region %s (just the Geometry)' % (subc_id, basin_id, reg_id))

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
    query = f'''
    SELECT
    ST_AsText(ST_LineInterpolatePoint(
        geom,
        ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint({lon}, {lat}),4326))
    ))
    FROM hydro.stream_segments
    WHERE
        subc_id = {subc_id}
        AND basin_id = {basin_id}
        AND reg_id = {reg_id}
    '''

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    cursor = conn.cursor()
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'basic snapping')

    ### Get results and construct GeoJSON:

    # Get row from database:
    row = cursor.fetchone();
    if row is None:
        LOGGER.warning("Received result_row None for point: lon=%s, lat=%s (subc_id %s). This is weird. Any point should be snappable, right?" % (lon, lat, subc_id))
        err_msg = "Weird: Could not snap point lon=%s, lat=%s" % (lon, lat) 
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)
        # Or return features with empty geometries:
        # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
        # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

    # Assemble GeoJSON to return:
    snappedpoint_simplegeom = geomet.wkt.loads(row[0])
    return snappedpoint_simplegeom


#############################
### Many points at a time ###
#############################

# Just a wrapper
def get_snapped_points_json2json(conn, points_geojson, colname_site_id = None):
    # INPUT: GeoJSON (Multipoint)
    # OUTPUT: FeatureCollection (Point)
    return get_snapped_point_xy(conn, geojson = points_geojson, colname_site_id = colname_site_id, result_format="geojson")


# Just a wrapper
def get_snapped_points_csv2csv(conn, input_df, colname_lon, colname_lat, colname_site_id):
    # INPUT: Pandas dataframe
    # OUTPUT: Pandas dataframe
    return get_snapped_point_xy(conn,
        dataframe = input_df,
        colname_lon = colname_lon,
        colname_lat = colname_lat,
        colname_site_id = colname_site_id,
        result_format="csv")


# Just a wrapper
def get_snapped_points_csv2json(conn, input_df, colname_lon, colname_lat, colname_site_id):
    # INPUT: Pandas dataframe
    # OUTPUT: FeatureCollection (Point)
    return get_snapped_point_xy(conn,
        dataframe = input_df,
        colname_lon = colname_lon,
        colname_lat = colname_lat,
        colname_site_id = colname_site_id,
        result_format="geojson")


# Just a wrapper
def get_snapped_points_json2csv(conn, points_geojson, colname_lon, colname_lat, colname_site_id):
    # INPUT: GeoJSON (Multipoint)
    # OUTPUT: Pandas dataframe
    return get_snapped_point_xy(conn,
        geojson = points_geojson,
        colname_site_id = colname_site_id,
        colname_lon = colname_lon,
        colname_lat = colname_lat,
        result_format="csv")


def get_snapped_point_xy(conn, geojson=None, dataframe=None, colname_lon=None, colname_lat=None, colname_site_id=None, result_format="geojson"):

    if dataframe is not None:
        LOGGER.debug('Basic snapping plural, based on input dataframe...')
        list_of_insert_rows = temp_table_for_queries.make_insertion_rows_from_dataframe(
            dataframe, colname_lon, colname_lat, colname_site_id)
    elif geojson is not None:
        LOGGER.debug('Basic snapping plural, based on input GeoJSON...')
        list_of_insert_rows = temp_table_for_queries.make_insertion_rows_from_geojson(
            geojson, colname_site_id)
    else:
        err_msg = 'Cannot recognize input object!'
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)

    # A temporary table is created and populated with the lines above.
    cursor = conn.cursor()
    tablename, reg_ids = temp_table_for_queries.create_and_populate_temp_table(
        cursor, list_of_insert_rows)

    # Then the points are snapped to those neighbouring stream segments:
    result_to_be_returned =  _run_snapping_query(cursor, tablename, reg_ids, result_format, colname_lon, colname_lat, colname_site_id)

    # Database hygiene: Drop the table
    temp_table_for_queries.drop_temp_table(cursor, tablename)
    return result_to_be_returned


def _run_snapping_query(cursor, tablename, reg_id_set, result_format, colname_lon, colname_lat, colname_site_id):
    ## This does not write anything into the database:
    LOGGER.debug('Performing the basic snapping on the temp table...')
    reg_ids_string = ','.join(map(str, reg_id_set))
    query = f'''
    SELECT
        poi.lon,
        poi.lat,
        poi.subc_id,
        poi.basin_id,
        poi.reg_id,
        seg.strahler,
        ST_AsText(ST_LineInterpolatePoint(
            seg.geom,
            ST_LineLocatePoint(seg.geom, poi.geom_user)
        )),
        poi.site_id
    FROM hydro.stream_segments seg, {tablename} poi
    WHERE
        seg.subc_id = poi.subc_id
        AND seg.reg_id IN ({reg_ids_string});
    '''

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'basic snapping (many points)')

    return _package_result(cursor, result_format, colname_lon, colname_lat, colname_site_id)


def _package_result(cursor, result_format, colname_lon, colname_lat, colname_site_id):
    result_to_be_returned = None

    if result_format == "geojson":
        result_to_be_returned = _package_result_in_geojson(cursor, colname_site_id)

    elif result_format == "csv":
        if colname_lon is None or colname_lat is None:
            raise UserInputException("Need to provide column names for lon and lat for the resulting dataframe!")
        result_to_be_returned = _package_result_in_dataframe(cursor, colname_lon, colname_lat, colname_site_id)

    return result_to_be_returned


def _package_result_in_geojson(cursor, colname_site_id):
    LOGGER.debug("Generating GeoJSON from database query result...")
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')

    # Create list to be filled with the GeoJSON Features:
    features = []

    # Iterating over database results:
    while (True):
        row = cursor.fetchone()
        if row is None: break

        # Extract values from row:
        lon = float(row[0])
        lat = float(row[1])
        subc_id = row[2]
        basin_id = row[3]
        reg_id = row[4]
        strahler = row[5]
        snappedpoint_wkt = row[6]
        site_id = row[7]

        # Convert to GeoJSON:
        snappedpoint_simplegeom = geomet.wkt.loads(snappedpoint_wkt)

        # Construct Feature, incl. ids, strahler and original lonlat:
        # TODO: SMALL: If all are in same reg_id and basin, we could remove those
        # attributes from here...
        feature = {
            "type": "Feature",
            "geometry": snappedpoint_simplegeom,
            "properties": {
                "subc_id": subc_id,
                "strahler": strahler,
                "basin_id": basin_id,
                "reg_id": reg_id,
                "lon_original": lon,
                "lat_original": lat,
            }
        }
        # Add site_id, if it was specified:
        if colname_site_id is not None:
            feature["properties"][colname_site_id] = site_id
        features.append(feature)


    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON... DONE.')

    if len(features) == 0:
        raise exc.UserInputException("No features...")

    feature_coll = {
        "type": "FeatureCollection",
        "features": features
    }
    LOGGER.log(logging.TRACE, 'Generated GeoJSON: %s' % feature_coll)
    return feature_coll


def _package_result_in_dataframe(cursor, colname_lon, colname_lat, colname_site_id):
    LOGGER.debug("Generating dataframe from database query result...")

    # Create list to be filled and converted to Pandas dataframe:
    everything = []

    # These will be the column names:
    colnames = [
        colname_site_id,
        'subc_id',
        'basin_id',
        'reg_id',
        'strahler',
        colname_lon+'_snapped',
        colname_lon+'_original',
        colname_lat+'_snapped',
        colname_lat+'_original'
    ]

    # Iterating over database results:
    while (True):
        row = cursor.fetchone()
        if row is None: break

        # Extract values from row:
        lon = float(row[0])
        lat = float(row[1])
        subc_id = row[2]
        basin_id = row[3]
        reg_id = row[4]
        strahler = row[5]
        snappedpoint_wkt = row[6]
        site_id = row[7]

        # Catch geometry problems, e.g. stream segment is NULL, then snapping will return None:
        # This happened for point  23.12695,37.8368 (subc_id 561594812, basin 1271669, region 66)
        if snappedpoint_wkt is None:
            err_msg = f"Point could not be snapped: lon lat = {lon}, {lat} ({colname_site_id} {site_id}, subcatchment {subc_id} in basin {basin_id}, region {reg_id})."
            LOGGER.error(err_msg)
            #raise ValueError(err_msg)

        # Convert to GeoJSON:
        try:
            snappedpoint_simplegeom = geomet.wkt.loads(snappedpoint_wkt)
            # Extract snapped coordinates:
            lon_snapped = snappedpoint_simplegeom['coordinates'][0]
            lat_snapped = snappedpoint_simplegeom['coordinates'][1]

        except StopIteration as e:
            err_msg = f"Failed to load geometry: {snappedpoint_wkt} (in row: {row})"
            LOGGER.error(f"{type(e).__name__}: {err_msg}")
            LOGGER.error(f"Error details: {type(e).__name__}: {e.args}")
            #raise ValueError(err_msg)
            lon_snapped = None
            lat_snapped = None


        # Append the line to dataframe:
        everything.append([site_id, subc_id, basin_id, reg_id, strahler, lon_snapped, lon, lat_snapped, lat])

    # Construct pandas dataframe from collected rows:
    output_dataframe = pd.DataFrame(everything, columns=colnames)
    # Dropping NA column: If colname_site_id is None, it led to a column named NA containing only NA.
    output_dataframe = output_dataframe.loc[:, output_dataframe.columns.notna()]
    return output_dataframe



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
        import aqua90m.geofresh.temp_table_for_queries as temp_table_for_queries
        from aqua90m.geofresh.temp_table_for_queries import log_query_time as log_query_time
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.utils.geojson_helpers as geojson_helpers
        import aqua90m.utils.exceptions as exc
        import aqua90m.geofresh.temp_table_for_queries as temp_table_for_queries
        from aqua90m.geofresh.temp_table_for_queries import log_query_time as log_query_time


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
    subc_id = 506251252
    basin_id = 1292547
    reg_id = 58

    print('\nSTART RUNNING FUNCTION: get_snapped_point_simplegeom')
    start = time.time()
    res = get_snapped_point_simplegeom(conn, lon, lat, subc_id, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_feature')
    start = time.time()
    res = get_snapped_point_feature(conn, lon, lat, subc_id, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_geometry_coll')
    start = time.time()
    res = get_snapped_point_geometry_coll(conn, lon, lat, subc_id, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_feature_coll')
    start = time.time()
    res = get_snapped_point_feature_coll(conn, lon, lat, subc_id, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)


    ####################################
    ### Run function for many points ###
    ### input GeoJSON                ###
    ####################################

    input_points_geojson = {
        "type": "MultiPoint",
        "coordinates": [
            [9.931555, 54.695070],
            [9.921555, 54.295070]
        ]
    }

    print('\nSTART RUNNING FUNCTION: get_snapped_points_1')
    start = time.time()
    res = get_snapped_points_json2json(conn, input_points_geojson)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT: %s' % res)

    input_points_geojson = {
        "type": "MultiPoint",
        "coordinates": [
            [9.931555, 54.695070],
            [9.921555, 54.295070],
            [47.630, 13.90],
            [50.250, 9.50],
            [9.931555, 54.695070],
            [9.921555, 54.295070],
            [47.630, 13.90],
            [50.250, 9.50]
        ]
    }

    print('\nSTART RUNNING FUNCTION: get_snapped_points_1, some more points...')
    start = time.time()
    res = get_snapped_points_json2json(conn, input_points_geojson)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT: %s' % res)

    input_points_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
               "type": "Feature",
               "geometry": { "type": "Point", "coordinates": [9.931555, 54.695070]},
               "properties": {
                   "my_site": "bla1",
                   "species_name": "Hase",
                   "species_id": "007"
               }
            },
            {
               "type": "Feature",
               "geometry": { "type": "Point", "coordinates": [9.921555, 54.295070]},
               "properties": {
                   "my_site": "bla2",
                   "species_name": "Delphin",
                   "species_id": "008"
               }
            }
        ]
    }


    print('\nTEST CUSTOM EXCEPTION: get_snapped_points_1, FeatureCollection...')
    try:
        res = get_snapped_points_json2json(conn, input_points_geojson)
        raise RuntimeError('Should not reach here!')
    except exc.UserInputException as e:
        print('RESULT: Proper exception, saying: %s' % e)


    print('\nSTART RUNNING FUNCTION: get_snapped_points_1, FeatureCollection...')
    start = time.time()
    res = get_snapped_points_json2json(conn, input_points_geojson, "my_site")
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT: %s' % res)

    input_points_geojson = {
        "type": "GeometryCollection",
        "geometries": [
            {
               "type": "Point",
               "coordinates": [9.931555, 54.695070]
            },
            {
               "type": "Point",
               "coordinates": [9.921555, 54.295070]
            }
        ]
    }

    print('\nSTART RUNNING FUNCTION: get_snapped_points_1, GeometryCollection...')
    start = time.time()
    res = get_snapped_points_json2json(conn, input_points_geojson)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_points_2, data_frame...')
    example_dataframe = pd.DataFrame(
        [
            ['aa', 10.041155219078064, 53.07006147583069],
            ['bb', 10.042726993560791, 53.06911450500803],
            ['cc', 10.039894580841064, 53.06869677412868],
            ['a',  10.698832912677716, 53.51710727672125],
            ['b',  12.80898022975407,  52.42187129944509],
            ['c',  11.915323076217902, 52.730867141970464],
            ['d',  16.651903948708565, 48.27779486850176],
            ['e',  19.201146608148463, 47.12192880511424],
            ['f',  24.432498016999062, 61.215505889934434]
        ], columns=['my_site', 'lon', 'lat']
    )
    start = time.time()
    res = get_snapped_points_csv2csv(conn, example_dataframe, 'lon', 'lat', 'my_site')
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT: %s' % res)
