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
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+'. If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)

# TODO: FUTURE: If we ever snap to stream segments outside of the immediate subcatchment,
# need to adapt some stuff in this process...


###########################
### One point at a time ###
###########################

def get_snapped_point_geometry_coll(conn, lon, lat, subc_id, basin_id, reg_id):
    # Just a wrapper!
    # INPUT: subc_id
    # OUTPUT: GeometryCollection (point, stream segment, connecting line)
    return _get_snapped_point_plus(conn, lon, lat, subc_id, basin_id, reg_id, make_feature = False)


def get_snapped_point_feature_coll(conn, lon, lat, subc_id, basin_id, reg_id):
    # Just a wrapper!
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
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

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
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

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


#############################
### Many points at a time ###
#############################

def get_snapped_points_1(conn, points_geojson, colname_site_id = None):
    # Just a wrapper
    # INPUT: GeoJSON (Multipoint)
    # OUTPUT: FeatureCollection (Point)

    # Check GeoJSON validity, and define what to iterate over:
    if points_geojson['type'] == 'GeometryCollection':
        geojson_helpers.check_is_geometry_collection_points(points_geojson)
        iterate_over = points_geojson['geometries']
        num = len(iterate_over)
    elif points_geojson['type'] == 'FeatureCollection':
        geojson_helpers.check_is_feature_collection_points(points_geojson)
        if colname_site_id is None:
            err_msg = "Please provide the property name where the site id is provided."
            LOGGER.error(err_msg)
            raise ValueError(err_msg)
        iterate_over = points_geojson['features']
        num = len(iterate_over)

    return get_snapped_point_xy(conn, geojson = points_geojson, colname_site_id = colname_site_id)

def get_snapped_points_2(conn, input_df, colname_lon, colname_lat, colname_site_id):
    # Just a wrapper
    # INPUT: Pandas dataframe
    # OUTPUT: Pandas dataframe
    return get_snapped_point_xy(conn,
        dataframe = input_df,
        colname_lon = colname_lon,
        colname_lat = colname_lat,
        colname_site_id = colname_site_id)

def get_snapped_point_xy(conn, geojson=None, dataframe=None, colname_lon=None, colname_lat=None, colname_site_id=None):
    # INPUT: GeoJSON (Multipoint)
    # OUTPUT: FeatureCollection (Point)

    cursor = conn.cursor()

    #######################
    ## Create temp table ##
    #######################

    # TODO WIP numeric or decimal or ...?
    # TODO: Is varchar a good type for expected site_ids?
    tablename = 'snapping_{uuid}'.format(uuid = str(uuid.uuid4()).replace('-', ''))
    LOGGER.debug('Creating temporary table "%s"...' % tablename)
    query_create = """
    CREATE TEMP TABLE {tablename} (
    site_id varchar(100),
    lon decimal,
    lat decimal,
    subc_id integer,
    basin_id integer,
    reg_id smallint,
    geom_user geometry(POINT, 4326)
    );
    """.format(tablename = tablename)
    query_create = query_create.replace("\n", " ")
    ## Run the create query:
    ## Note: At first, we ran them all at once, but for measuring performance we now
    ## send them separately, and it does not make things much slower.
    _start = time.time()
    cursor.execute(query_create)
    _end = time.time()
    LOGGER.log(logging.TRACE, '**** TIME ************ query_create: %s' % (_end - _start))

    ## Collect the user values
    tmp = []

    ## Collect the user values from dataframe:
    if dataframe is not None:
        for row in dataframe.itertuples(index=False):
            lon = getattr(row, colname_lon)
            lat = getattr(row, colname_lat)
            site_id = getattr(row, colname_site_id)
            tmp.append("('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))".format(site_id=site_id, lon=lon, lat=lat))

    ## Collect the user values from GeoJSON:
    elif geojson is not None:
        if geojson['type'] == 'MultiPoint':
            site_id = 'none' # TODO Maybe fill with NULL values, or not create that column if it is not needed? 
            for lon, lat in geojson["coordinates"]:
                tmp.append("('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))".format(site_id=site_id, lon=lon, lat=lat))
        elif geojson['type'] == 'GeometryCollection':
            site_id = 'none'
            for point in geojson['geometries']:
                lon, lat = point['coordinates']
                tmp.append("('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))".format(site_id=site_id, lon=lon, lat=lat))
        elif geojson['type'] == 'FeatureCollection':
            for point in geojson['features']:
                lon, lat = point['geometry']['coordinates']
                site_id = point['properties'][colname_site_id]
                tmp.append("('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))".format(site_id=site_id, lon=lon, lat=lat))
        else:
            err_msg = 'Cannot recognize GeoJSON object!'
            LOGGER.error(err_msg)
            raise ValueError(err_msg)
    else:
        err_msg = 'Cannot recognize input object!'
        LOGGER.error(err_msg)
        raise ValueError(err_msg)

    ## Insert the user values
    LOGGER.debug('Inserting into temporary table "%s"...' % tablename)
    query_insert = "INSERT INTO {tablename} (site_id, lon, lat, geom_user) VALUES {values};".format(tablename=tablename, values=", ".join(tmp))
    _start = time.time()
    cursor.execute(query_insert)
    _end = time.time()
    LOGGER.log(logging.TRACE, '**** TIME ************ query_insert: %s' % (_end - _start))

    # Adding index:
    LOGGER.debug('Creating index for temporary table "%s"...' % tablename)
    query_index = "CREATE INDEX IF NOT EXISTS temp_test_geom_user_idx ON {tablename} USING gist (geom_user);".format(tablename=tablename)
    _start = time.time()
    cursor.execute(query_index)
    _end = time.time()
    LOGGER.log(logging.TRACE, '**** TIME ************ query_index: %s' % (_end - _start))

    ## Add reg_id to temp table, get it returned:
    LOGGER.debug('Update reg_id (st_intersects) in temporary table "%s"...' % tablename)
    query_reg = "WITH updater AS (UPDATE {tablename} SET reg_id = reg.reg_id FROM regional_units reg WHERE st_intersects({tablename}.geom_user, reg.geom) RETURNING {tablename}.reg_id) SELECT DISTINCT reg_id FROM updater;".format(tablename = tablename)
    _start = time.time()
    cursor.execute(query_reg)
    _end = time.time()
    LOGGER.log(logging.TRACE, '**** TIME ************ query_reg: %s' % (_end - _start))

    ## Retrieve reg_id, for next query:
    LOGGER.log(logging.TRACE, 'Retrieving reg_ids (RETURNING from UPDATE query)...')
    reg_id_set = set()
    while (True):
        row = cursor.fetchone()
        if row is None: break
        LOGGER.log(logging.TRACE, '  Retrieved: %s' % str(row[0]))
        reg_id_set.add(row[0])
    LOGGER.debug('Set of reg_ids: %s' % reg_id_set)

    ## Add sub_id:
    LOGGER.debug('Update subc_id, basin_id (st_intersects) in temporary table "%s"...' % tablename)
    reg_ids_string = ", ".join([str(elem) for elem in reg_id_set])
    _start = time.time()
    query_sub_bas = "UPDATE {tablename} SET subc_id = sub.subc_id, basin_id = sub.basin_id FROM sub_catchments sub WHERE st_intersects({tablename}.geom_user, sub.geom) AND sub.reg_id IN ({reg_ids}) ;".format(tablename = tablename, reg_ids = reg_ids_string)
    cursor.execute(query_sub_bas)
    _end = time.time()
    LOGGER.log(logging.TRACE, '**** TIME ************ query_sub_bas: %s' % (_end - _start))
    LOGGER.log(logging.TRACE, 'Creating temporary table "%s"... DONE.' % tablename)

    ############################
    ## Run the snapping query ##
    ############################

    ## This does not write anything into the database:
    reg_ids_string = ", ".join([str(elem) for elem in reg_id_set])
    query_snap = '''
    SELECT
    poi.lon,
    poi.lat,
    poi.subc_id,
    poi.basin_id,
    poi.reg_id,
    seg.strahler,
    ST_AsText(ST_LineInterpolatePoint(seg.geom, ST_LineLocatePoint(seg.geom, poi.geom_user))),
    poi.site_id
    FROM hydro.stream_segments seg, {tablename} poi
    WHERE seg.subc_id = poi.subc_id AND seg.reg_id IN ({reg_ids});
    '''.format(tablename = tablename, reg_ids = reg_ids_string)
    # TODO: Add ST_AsText(seg.geom) if you want the linestring!
    query_snap = query_snap.replace("\n", " ")
    LOGGER.debug('Querying database with snapping query...')
    _start = time.time()
    cursor.execute(query_snap)
    _end = time.time()
    LOGGER.log(logging.TRACE, '**** TIME ************ query_snap: %s' % (_end - _start))
    LOGGER.debug('Querying database with snapping query... DONE.')

    ## Now iterate over result rows:
    result_to_be_returned = None
    # If input was GeoJSON, we return output as GeoJSON:
    if geojson is not None:

        LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
        features = []
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
            #streamsegment_wkt = row[7]

            # Convert to GeoJSON:
            snappedpoint_simplegeom = geomet.wkt.loads(snappedpoint_wkt)
            #streamsegment_linestring = geomet.wkt.loads(streamsegment_wkt)

            # Construct Feature, incl. ids, strahler and original lonlat:
            # TODO: If all are in same reg_id and basin, we could remove those
            # attributes from here...
            # TODO: If the input was a Geometry or GeometryCollection, we have no site_id!
            features.append({
                "type": "Feature",
                "geometry": snappedpoint_simplegeom,
                "properties": {
                    colname_site_id: site_id,
                    "subc_id": subc_id,
                    "strahler": strahler,
                    "basin_id": basin_id,
                    "reg_id": reg_id,
                    "lon_original": lon,
                    "lat_original": lat,
                }
            })
        LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON... DONE.')

        if len(features) == 0:
            raise ValueError("No features...")

        feature_coll = {
            "type": "FeatureCollection",
            "features": features
        }
        LOGGER.log(logging.TRACE, 'Generated GeoJSON: %s' % feature_coll)
        result_to_be_returned = feature_coll

    # If input was dataframe, we return output as dataframe:
    elif dataframe is not None:
        # Create list to be filled and converted to Pandas dataframe:
        everything = []
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
            #streamsegment_wkt = row[7]
            snappedpoint_simplegeom = geomet.wkt.loads(snappedpoint_wkt)
            lon_snapped = snappedpoint_simplegeom['coordinates'][0]
            lat_snapped = snappedpoint_simplegeom['coordinates'][1]
            everything.append([site_id, subc_id, basin_id, reg_id, strahler, lon_snapped, lon, lat_snapped, lat])


        output_dataframe = pd.DataFrame(everything, columns=[
            colname_site_id,
            'subc_id', 'basin_id', 'reg_id',
            'strahler',
            colname_lon+'_snapped', colname_lon+'_original',
            colname_lat+'_snapped', colname_lat+'_original'
        ])
        result_to_be_returned = output_dataframe




    ## Drop temp table:
    LOGGER.debug('Dropping temporary table "%s".' % tablename)
    query_drop = "DROP TABLE IF EXISTS {tablename};".format(tablename = tablename)
    cursor.execute(query_drop)

    return result_to_be_returned


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
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.utils.geojson_helpers as geojson_helpers


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
    res = get_snapped_points_1(conn, input_points_geojson)
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
    res = get_snapped_points_1(conn, input_points_geojson)
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

    print('\nSTART RUNNING FUNCTION: get_snapped_points_1, FeatureCollection...')
    start = time.time()
    res = get_snapped_points_1(conn, input_points_geojson, "my_site")
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
    res = get_snapped_points_1(conn, input_points_geojson)
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
    res = get_snapped_points_2(conn, example_dataframe, 'lon', 'lat', 'my_site')
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT: %s' % res)
