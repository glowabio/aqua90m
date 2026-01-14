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

###########################
### One point at a time ###
###########################

# Just a wrapper!
def get_snapped_point_geometry_coll(conn, lon, lat, strahler, basin_id, reg_id):
    # INPUT: lon, lat
    # OUTPUT: GeometryCollection (point, stream segment, connecting line)
    return _get_snapped_point_plus(conn, lon, lat, strahler, basin_id, reg_id, make_feature = False)


# Just a wrapper!
def get_snapped_point_feature_coll(conn, lon, lat, strahler, basin_id, reg_id):
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
    query = f'''
    SELECT 
        ST_AsText(ST_LineInterpolatePoint(
            closest.geom,
            ST_LineLocatePoint(closest.geom, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))
        )),
        ST_AsText(closest.geom),
        closest.strahler,
        closest.subc_id
    FROM (
        SELECT 
            seg.subc_id,
            seg.strahler,
            seg.geom AS geom,
            seg.geom <-> ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geometry AS dist
        FROM hydro.stream_segments seg
        WHERE seg.strahler >= {strahler}
        ORDER BY dist
        LIMIT 1
    ) AS closest;
    '''.replace("\n", " ")

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    cursor = conn.cursor()
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'snapping-strahler-plus for one point')

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


###############################
### Many points at a time   ###
### Functions to be exposed ###
###############################

# Just a wrapper!
def get_snapped_points_json2json(conn, points_geojson, min_strahler, colname_site_id=None, add_distance=None):
    # INPUT: GeoJSON (Multipoint)
    # OUTPUT: FeatureCollection (Point)
    return get_snapped_points_xy(conn, geojson = points_geojson, min_strahler = min_strahler, colname_site_id = colname_site_id, result_format="geojson", add_distance=add_distance)


# Just a wrapper!
def get_snapped_points_csv2csv(conn, input_df, min_strahler, colname_lon, colname_lat, colname_site_id, add_distance=None):
    # INPUT: Pandas dataframe
    # OUTPUT: Pandas dataframe
    return get_snapped_points_xy(conn,
        dataframe = input_df,
        colname_lon = colname_lon,
        colname_lat = colname_lat,
        colname_site_id = colname_site_id,
        result_format="csv",
        min_strahler=min_strahler,
        add_distance=add_distance)


# Just a wrapper!
def get_snapped_points_csv2json(conn, input_df, min_strahler, colname_lon, colname_lat, colname_site_id, add_distance=None):
    # INPUT: Pandas dataframe
    # OUTPUT: FeatureCollection (Point)
    return get_snapped_points_xy(conn,
        dataframe = input_df,
        colname_lon = colname_lon,
        colname_lat = colname_lat,
        colname_site_id = colname_site_id,
        result_format="geojson",
        min_strahler=min_strahler,
        add_distance=add_distance)


# Just a wrapper!
def get_snapped_points_json2csv(conn, points_geojson, min_strahler, colname_lon, colname_lat, colname_site_id, add_distance=None):
    # INPUT: GeoJSON (Multipoint)
    # OUTPUT: Pandas dataframe
    return get_snapped_points_xy(conn,
        geojson = points_geojson,
        colname_site_id = colname_site_id,
        colname_lon = colname_lon,
        colname_lat = colname_lat,
        result_format="csv",
        min_strahler=min_strahler,
        add_distance=add_distance)


##################################
### Many points at a time      ###
### Functions that do the work ###
##################################

def get_snapped_points_xy(conn, geojson=None, dataframe=None, colname_lon=None, colname_lat=None, colname_site_id=None, min_strahler=1, add_distance=True, result_format="geojson"):

    if min_strahler is None:
        raise ValueError('Must provide min_strahler')
    if add_distance is None:
        raise ValueError('Must provide add_distance')
    LOGGER.debug(f'Snapping to min strahler order: "{min_strahler}".')

    # The input passed by the user is converted to SQL rows
    # that can be inserted into a temporary table:
    if dataframe is not None:
        list_of_insert_rows = temp_table_for_queries.make_insertion_rows_from_dataframe(
            dataframe, colname_lon, colname_lat, colname_site_id)
    elif geojson is not None:
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

    # Then, the nearest-neighbouring stream segments are added:
    _add_nearest_neighours_to_temptable(cursor, tablename, min_strahler)

    if add_distance:
        result_to_be_returned = _snapping_with_distances(cursor, tablename, result_format, colname_lon, colname_lat, colname_site_id)
    else:
        # Then the points are snapped to those neighbouring stream segments:
        result_to_be_returned =  _snapping_without_distances(cursor, tablename, result_format, colname_lon, colname_lat, colname_site_id)

    # Database hygiene: Drop the table
    temp_table_for_queries.drop_temp_table(cursor, tablename)
    return result_to_be_returned


def _add_nearest_neighours_to_temptable(cursor, tablename, min_strahler):
    # Fill the table with the geometry and properties of the nearest neighbour
    # stream segment. For this we compute the distance using <->, and sort by that.
    LOGGER.debug(f'Adding nearest neighbours to temporary table "{tablename}"...')

    # Note: The columns we UPDATE here (geom_closest, strahler_closest, subcid_closest)
    # have to exist in the temp table!
    query = f'''
    ALTER TABLE {tablename}
        ADD COLUMN geom_closest geometry(LINESTRING, 4326),
        ADD COLUMN subcid_closest integer,
        ADD COLUMN strahler_closest integer;
    '''.replace("\n", "")
    cursor.execute(query)

    # Note: LATERAL makes the subquery run once per row of tablename.
    # Note: In the WHERE clause, we match based on the point geometry passed
    # by the user. Site_id could be another candidate. Previously, we used
    # subc_id, BUT not all points get assigned a subc_id, basin_id, reg_id.
    # Those that fall in the ocean - or on the coast just slightly outside the
    # polygons of table "reg" - don't have one. By using "geom_user", they are
    # snapped anyway, and the user has to decide whether they are land or sea...
    # Old "WHERE": ... WHERE temp1.subc_id = temp2.subc_id;
    query = f'''
    UPDATE {tablename} AS temp1
    SET
        geom_closest = closest.geom,
        strahler_closest = closest.strahler,
        subcid_closest = closest.subc_id
    FROM {tablename} AS temp2
    CROSS JOIN LATERAL (
        SELECT seg.geom, seg.strahler, seg.subc_id
        FROM stream_segments seg
        WHERE seg.strahler >= {min_strahler}
        ORDER BY seg.geom <-> ST_SetSRID(ST_MakePoint(temp2.lon, temp2.lat), 4326)
        LIMIT 1
    ) AS closest
    WHERE temp1.geom_user = temp2.geom_user;
    '''.replace("\n", " ")

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'adding nearest neighbours')
    LOGGER.debug(f'Adding nearest neighbours to temporary table "{tablename}"... done.')


def _snapping_with_distances(cursor, tablename, result_format, colname_lon, colname_lat, colname_site_id):
    # Compute the snapped point, store in table, and calculate distance.

    # Add column for snapped point:
    LOGGER.debug(f'Adding snapped points to temporary table "{tablename}"...')
    query = f'ALTER TABLE {tablename} ADD COLUMN geom_snapped geometry(POINT, 4326)'
    cursor.execute(query)

    # Compute snapped point, store in table:
    query = f'''
    UPDATE {tablename} AS temp
        SET geom_snapped = ST_LineInterpolatePoint(
            temp.geom_closest,
            ST_LineLocatePoint(temp.geom_closest, temp.geom_user)
        );
    '''.replace("\n", " ")

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'computing snapped points and store in table')
    LOGGER.debug(f'Adding snapped points to temporary table "{tablename}"... done.')

    # Compute the distance, retrieve the snapped points:
    # Note: ST_Distance operates on WGS84 and returns degrees, so we
    # cast to a "geography", see explanation here:
    # https://www.postgis.net/workshops/postgis-intro/geography.html
    LOGGER.debug(f'Retrieving snapped points from temporary table "{tablename}"...')
    query = f'''
    SELECT
        temp.lon,
        temp.lat,
        temp.site_id,
        ST_AsText(temp.geom_snapped),
        temp.strahler_closest,
        temp.subcid_closest,
        ST_Distance(
            temp.geom_user::geography,
            temp.geom_snapped::geography
        )
    FROM {tablename} AS temp;
    '''.replace("\n", " ")

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'computing distances and retrieve results')
    return _package_result(cursor, result_format, colname_lon, colname_lat, colname_site_id)


def _snapping_without_distances(cursor, tablename, result_format, colname_lon, colname_lat, colname_site_id):
    # Run the query that generates the snapped point, i.e. the point on the
    # stream segment (nearest neighbour) that is closest to the original point.
    # The nearest-neighbouring stream segments in question have been previously
    # found and stored to column "geom_closest" by the previous query.
    # (So this here is pretty much the normal snapping query).
    #
    # This RETURNS the snapped points, but does not STORE them in the temp table!

    query = f'''
    SELECT
        temp.lon,
        temp.lat,
        temp.site_id,
        ST_AsText(
            ST_LineInterpolatePoint(
                temp.geom_closest,
                ST_LineLocatePoint(temp.geom_closest, ST_SetSRID(ST_MakePoint(temp.lon, temp.lat), 4326))
            )
        ),
        temp.strahler_closest,
        temp.subcid_closest
    FROM {tablename} AS temp
    '''.replace("\n", " ")

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'snapping without distances')
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
    LOGGER.debug("Generating GeoJSON to return...")
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
        site_id = row[2]
        snappedpoint_wkt = row[3]
        strahler = row[4]
        subc_id = row[5]
        try:
            distance_metres = row[6] # optional
        except IndexError as e:
            distance_metres = None

        # For debugging, add any attribute to the last SELECT statement, and look at all of them here:
        LOGGER.log(logging.TRACE, f'Result row: {row}')

        # Convert to GeoJSON:
        if snappedpoint_wkt is None:
            # If point is in the ocean...
            LOGGER.debug(f'This point has no ids assigned, so it may be off the coast: site_id={site_id}, lon={lon}, lat={lat}.')
            snappedpoint_simplegeom = None
        else:
            snappedpoint_simplegeom = geomet.wkt.loads(snappedpoint_wkt)

        # Construct Feature:
        feature = {
            "type": "Feature",
            "geometry": snappedpoint_simplegeom,
            "properties": {
                "lon_original": lon,
                "lat_original": lat,
                "strahler": strahler,
                "subc_id": subc_id
            }
        }
        # Add site_id, if it was specified:
        if colname_site_id is not None:
            feature["properties"][colname_site_id] = site_id

        # Add distance, if it was computed:
        if distance_metres is not None:
            feature["properties"]["distance_metres"] = distance_metres

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
    LOGGER.debug("Generating dataframe to return...")

    # Create list to be filled and converted to Pandas dataframe:
    everything = []

    # These will be the column names:
    colnames = [
        colname_site_id,
        'subc_id',
        'strahler',
        colname_lon+'_snapped',
        colname_lon+'_original',
        colname_lat+'_snapped',
        colname_lat+'_original',
        'distance_metres'
    ]

    # Iterating over database results:
    while (True):
        row = cursor.fetchone()
        if row is None: break

        # Extract values from row:
        lon = float(row[0])
        lat = float(row[1])
        site_id = row[2]
        snappedpoint_wkt = row[3]
        strahler = row[4]
        subc_id = row[5]
        try:
            distance_metres = row[6] # optional
        except IndexError as e:
            distance_metres = None

        # For debugging, add any attribute to the last SELECT statement, and look at all of them here:
        LOGGER.log(logging.TRACE, f'Result row: {row}')

        # Convert to GeoJSON:
        if snappedpoint_wkt is None:
            # If point is in the ocean...
            LOGGER.debug(f'This point has no ids assigned, so it may be off the coast: site_id={site_id}, lon={lon}, lat={lat}.')
            snappedpoint_simplegeom = None
            lon_snapped = None
            lat_snapped = None
        else:
            snappedpoint_simplegeom = geomet.wkt.loads(snappedpoint_wkt)
            # Extract snapped coordinates:
            lon_snapped = snappedpoint_simplegeom['coordinates'][0]
            lat_snapped = snappedpoint_simplegeom['coordinates'][1]

        # Append the line to dataframe:
        everything.append([
            site_id,
            subc_id,
            strahler,
            lon_snapped,
            lon,
            lat_snapped,
            lat,
            distance_metres
        ])

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



####################################
### Run function for many points ###
####################################

if __name__ == "__main__" and True:

    min_strahler = 5

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

    res = get_snapped_points_xy(conn,
        geojson=input_points_geojson,
        dataframe=None,
        colname_lon=None,
        colname_lat=None,
        colname_site_id="my_site",
        result_format="geojson",
        min_strahler=min_strahler
    )
    print('RESULT:')
    print(res)

    res = get_snapped_points_xy(conn,
        geojson=input_points_geojson,
        dataframe=None,
        colname_lon="lon",
        colname_lat="lat",
        colname_site_id="my_site",
        result_format="csv",
        min_strahler=min_strahler
    )
    print('RESULT:')
    print(res)


    res = get_snapped_points_json2csv(conn,
        input_points_geojson,
        min_strahler,
        "lon",
        "lat",
        "my_site"
    )
    print('RESULT:')
    print(res)

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

    res = get_snapped_points_csv2json(conn,
        example_dataframe,
        min_strahler,
        "lon", "lat",
        "my_site"
    )
    print('RESULT:')
    print(res)

    res = get_snapped_points_csv2csv(conn,
        example_dataframe,
        min_strahler,
        "lon", "lat",
        "my_site"
    )
    print('RESULT:')
    print(res)

    res = get_snapped_points_json2json(conn,
        input_points_geojson,
        min_strahler,
        "my_site"
    )
    print('RESULT:')
    print(res)


######################################
### Run function for single points ###
######################################

if __name__ == "__main__" and True:

    lon = 9.931555
    lat = 54.695070
    strahler = 1
    #subc_id = 506251252
    basin_id = 1292547
    reg_id = 58
    min_strahler = 5


    print('\nSTART RUNNING FUNCTION: get_snapped_point_simplegeom')
    start = time.time()
    res = get_snapped_point_geometry_coll(conn, lon, lat, min_strahler, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_snapped_point_feature')
    start = time.time()
    res = get_snapped_point_feature_coll(conn, lon, lat, min_strahler, basin_id, reg_id)
    end = time.time()
    print('TIME: %s' % (end - start))
    print('RESULT:\n%s' % res)


if __name__ == "__main__":
    conn.close()
