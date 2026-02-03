import json
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
import geomet.wkt
import pandas as pd
LOGGER = logging.getLogger(__name__)


try:
    # If the package is installed in local python PATH:
    import aqua90m.utils.extent_helpers as extent_helpers
    import aqua90m.utils.geojson_helpers as geojson_helpers
    import aqua90m.utils.exceptions as exc
    import aqua90m.geofresh.temp_table_for_queries as temp_tables
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.extent_helpers as extent_helpers
        import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
        import pygeoapi.process.aqua90m.utils.exceptions as exc
        import pygeoapi.process.aqua90m.geofresh.temp_table_for_queries as temp_tables
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)


#####################################
### Functions for singular points ###
### (all are covered by tests)    ###
#####################################

def get_regid(conn, LOGGER, lon=None, lat=None, subc_id=None):

    # Standard case: User provided lon and lat!
    if lon is not None and lat is not None:
        return get_regid_from_lonlat(conn, LOGGER, lon, lat)

    # Non-standard case: If user provided subc_id, then use it!
    if subc_id is not None:
        return get_regid_from_subcid(conn, LOGGER, subc_id)

def get_regid_from_lonlat(conn, LOGGER, lon, lat):

    #extent_helpers.check_outside_europe(lon, lat)
    # May throw OutsideAreaException/UserInputException
    # TODO: Can we find a more elegant solution for this?

    ### Define query:
    """
    Example query:
    SELECT reg_id FROM regional_units
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326), geom);

    Result:
     reg_id 
    --------
         58
    (1 row)
    """
    query = f'''
    SELECT reg_id
    FROM regional_units
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({lon}, {lat}),4326), geom)
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None: # Ocean case:
        err_msg = f'No reg_id found for lon {lon}, lat {lat}! Is this in the ocean?'
        LOGGER.warning(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)

    else:
        reg_id = row[0]

    return reg_id

def get_regid_from_subcid(conn, LOGGER, subc_id):
    # This function is not really useful, it's just here for the sake for systematicness.
    basin_id, reg_id = get_basinid_regid_from_subcid(conn, LOGGER, subc_id)
    return reg_id


def get_basinid_regid(conn, LOGGER, lon=None, lat=None, subc_id=None):

    # Standard case: User provided lon and lat!
    if lon is not None and lat is not None:
        return get_basinid_regid_from_lonlat(conn, LOGGER, lon, lat)

    # Non-standard case: If user provided subc_id, then use it!
    if subc_id is not None:
        return get_basinid_regid_from_subcid(conn, LOGGER, subc_id)

def get_basinid_regid_from_lonlat(conn, LOGGER, lon, lat):

    #extent_helpers.check_outside_europe(lon, lat)
    # May throw OutsideAreaException/UserInputException
    # TODO: Can we find a more elegant solution for this?

    ### Define query:
    """
    Example query:
    SELECT basin_id FROM basins
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070), 4326), geom);

    Result: TODO
    """
    query = f'''
    SELECT basin_id, reg_id
    FROM basins
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326), geom)
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None: # Ocean case:
        err_msg = f'No basin_id found for lon {lon}, lat {lat}! Is this in the ocean?'
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)

    else:
        basin_id = row[0]
        reg_id = row[1]

    return basin_id, reg_id

def get_basinid_regid_from_subcid(conn, LOGGER, subc_id):
    # TODO: We need this in plural for geofresh.get_env90m_data_for_subcids.py

    ### Define query:
    query = f'''
    SELECT basin_id, reg_id
    FROM sub_catchments
    WHERE subc_id = {subc_id}
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None:
        err_msg = f'No basin_id and reg_id found for subc_id {subc_id}!'
        LOGGER.error(err_msg)
        raise exc.GeoFreshUnexpectedResultException(err_msg)
    else:
        basin_id = row[0]
        reg_id = row[1]

    return basin_id, reg_id


def get_subcid_basinid_regid(conn, LOGGER, lon=None, lat=None, subc_id=None):

    # Standard case: User provided lon and lat!
    if lon is not None and lat is not None:
        return get_subcid_basinid_regid_from_lonlat(conn, LOGGER, lon, lat)

    # Non-standard case: If user provided subc_id, then use it!
    if subc_id is not None:
        return get_subcid_basinid_regid_from_subcid(conn, LOGGER, subc_id)

def get_subcid_basinid_regid_from_subcid(conn, LOGGER, subc_id):

    LOGGER.log(logging.TRACE, f'Getting subcatchment, region and basin id for subc_id: {subc_id}')
    basin_id, reg_id = get_basinid_regid_from_subcid(conn, LOGGER, subc_id)
    return subc_id, basin_id, reg_id

def get_subcid_basinid_regid_from_lonlat(conn, LOGGER, lon, lat):

    LOGGER.log(logging.TRACE, f'Getting subcatchment, region and basin id for lon, lat: {lon}, {lat}')
    lon = float(lon)
    lat = float(lat)
    reg_id = get_regid_from_lonlat(conn, LOGGER, lon, lat)
    subc_id, basin_id = get_subcid_basinid_from_lonlat_regid(conn, LOGGER, lon, lat, reg_id)
    LOGGER.log(logging.TRACE, f'Subcatchment has subc_id {subc_id}, basin_id {basin_id}, reg_id {reg_id}.')
    return subc_id, basin_id, reg_id


def get_subcid_basinid_from_lonlat_regid(conn, LOGGER, lon, lat, reg_id):

    ### Define query:
    """
    Example query:
    SELECT sub.subc_id, sub.basin_id FROM sub_catchments sub
    WHERE st_intersects(ST_SetSRID(ST_MakePoint(9.931555, 54.695070),4326), sub.geom)
    AND sub.reg_id = 58;

    Result:
    subc_id    | basin_id
    -----------+----------
     506251252 |  1292547
    (1 row)
    """

    query = f'''
    SELECT
        subc_id,
        basin_id
    FROM sub_catchments
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({lon}, {lat}),4326), geom)
        AND reg_id = {reg_id}
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results:
    row = cursor.fetchone()
    if row is None: # Ocean case:
        err_msg = f'No subc_id and basin_id found for lon {lon}, lat {lat}! Is this in the ocean?'
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)
    else:
        subc_id = row[0]
        basin_id = row[1]

    return subc_id, basin_id


def get_regid_from_basinid(conn, LOGGER, basin_id):

    ### Define query:
    query = f'''
    SELECT reg_id
    FROM hydro.basins
    WHERE basin_id = {basin_id}
    '''

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None:
        err_msg = f'No reg_id found for basin_id {basin_id}!'
        LOGGER.error(err_msg)
        raise exc.GeoFreshUnexpectedResultException(err_msg)
    else:
        reg_id = row[0]

    return reg_id


def get_all_subcids_from_basinid(conn, LOGGER, basin_id, reg_id, min_strahler=None):

    # Define query:
    if min_strahler is None:
        query = f'''
        SELECT subc_id, strahler
        FROM hydro.stream_segments
        WHERE basin_id = {basin_id}
            AND reg_id = {reg_id}
        '''
    else:
        query = f'''
        SELECT subc_id, strahler
        FROM hydro.stream_segments
        WHERE basin_id = {basin_id}
            AND reg_id = {reg_id}
            AND strahler >= {min_strahler}
        '''

    # Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    # Get results:
    subc_ids = [row[0] for row in cursor.fetchall()]

    # Complain if no subcatchments:
    if len(subc_ids) == 0:
        err_msg = f'No subcatchments found for basin_id {basin_id},'
        if min_strahler is None:
            err_msg += ' (any strahler order)!'
        else:
            err_msg += f' (strahler >= {min_strahler})!'
        LOGGER.error(err_msg)
        raise exc.GeoFreshUnexpectedResultException(err_msg)

    return subc_ids

#################################
### for many points at a time ###
### without loops/iteration   ###
#################################


def get_subcid_basinid_regid__dataframe_to_dataframe(conn, input_df, colname_lon, colname_lat, colname_site_id=None):
    # INPUT:  Dataframe with site_id, lon, lat
    # OUTPUT: Dataframe with site_id, subc_id, basin_id, reg_id
    list_of_insert_rows = temp_tables.make_insertion_rows_from_dataframe(input_df, colname_lon, colname_lat, colname_site_id)
    cursor = conn.cursor()
    tablename, reg_ids = temp_tables.create_and_populate_temp_table(cursor, list_of_insert_rows)
    if colname_site_id is None:
        query = f'''
        SELECT lon, lat, subc_id, basin_id, reg_id
        FROM {tablename}
        '''
    else:
        query = f'''
        SELECT site_id, lon, lat, subc_id, basin_id, reg_id
        FROM {tablename}
        '''
    output_df = pd.read_sql_query(query, conn)
    # TODO: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
    temp_tables.drop_temp_table(cursor, tablename)
    # Apparently, pd.read_sql_query() casts to numeric to be safe.
    # Casting back to int:
    output_df = output_df.astype({
        "subc_id": "Int64",
        "basin_id": "Int64",
        "reg_id": "Int64"
    })
    return output_df


def get_subcid_basinid_regid__geojson_to_dataframe(conn, input_geojson, colname_site_id=None):
    # INPUT:  GeoJSON (MultiPoint or GeometryCollection or FeatureCollection)
    # OUTPUT: Dataframe with site_id, subc_id, basin_id, reg_id
    # Note: If no colname_site_id is given, we can return the dataframe, but it
    # cannot be matched to the input points.
    list_of_insert_rows = temp_tables.make_insertion_rows_from_geojson(input_geojson, colname_site_id=colname_site_id)
    cursor = conn.cursor()
    tablename, reg_ids = temp_tables.create_and_populate_temp_table(cursor, list_of_insert_rows)
    if colname_site_id is None:
        query = f'''
        SELECT lon, lat, subc_id, basin_id, reg_id
        FROM {tablename}
        '''
    else:
        query = f'''
        SELECT site_id, lon, lat, subc_id, basin_id, reg_id
        FROM {tablename}
        '''
    output_df = pd.read_sql_query(query, conn)
    # TODO: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
    temp_tables.drop_temp_table(cursor, tablename)
    # Apparently, pd.read_sql_query() casts to numeric to be safe.
    # Casting back to int:
    output_df = output_df.astype({
        "subc_id": "Int64",
        "basin_id": "Int64",
        "reg_id": "Int64"
    })
    return output_df


def get_regid__dataframe_to_dataframe(conn, input_df, colname_lon, colname_lat, colname_site_id=None):
    # INPUT:  Dataframe with lon, lat, possibly site_id
    # OUTPUT: Dataframe with lon, lat, reg_id, possibly site_id
    list_of_insert_rows = temp_tables.make_insertion_rows_from_dataframe(input_df, colname_lon, colname_lat, colname_site_id)
    cursor = conn.cursor()
    tablename, reg_ids = temp_tables.create_and_populate_temp_table(cursor, list_of_insert_rows, add_subcids=False)
    if colname_site_id is None:
        query = f'''
        SELECT lon, lat, reg_id
        FROM {tablename}
        '''
    else:
        query = f'''
        SELECT site_id, lon, lat, reg_id
        FROM {tablename}
        '''
    output_df = pd.read_sql_query(query, conn)
    # TODO: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
    temp_tables.drop_temp_table(cursor, tablename)
    # Apparently, pd.read_sql_query() casts to numeric to be safe.
    # Casting back to int:
    output_df = output_df.astype({
        "reg_id": "Int64"
    })
    return output_df


def get_regid__geojson_to_dataframe(conn, input_geojson, colname_site_id=None):
    # INPUT:  GeoJSON (MultiPoint or GeometryCollection or FeatureCollection)
    # OUTPUT: Dataframe with site_id, reg_id
    # Note: If no colname_site_id is given, we can return the dataframe, but it
    # cannot be matched to the input points.
    list_of_insert_rows = temp_tables.make_insertion_rows_from_geojson(input_geojson, colname_site_id=colname_site_id)
    cursor = conn.cursor()
    tablename, reg_ids = temp_tables.create_and_populate_temp_table(cursor, list_of_insert_rows, add_subcids=False)
    if colname_site_id is None:
        query = f'''
        SELECT lon, lat, reg_id
        FROM {tablename}
        '''
    else:
        query = f'''
        SELECT site_id, lon, lat, reg_id
        FROM {tablename}
        '''
    output_df = pd.read_sql_query(query, conn)
    # TODO: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
    temp_tables.drop_temp_table(cursor, tablename)
    # Apparently, pd.read_sql_query() casts to numeric to be safe.
    # Casting back to int:
    output_df = output_df.astype({
        "reg_id": "Int64"
    })
    return output_df


def get_basinid_regid_from_subcid_plural(conn, subc_ids, columns=['subc_id', 'basin_id', 'reg_id']):
    # INPUT:  List of subc_ids (integers)
    # OUTPUT: Dataframe with subc_id, basin_id, reg_id
    # Note: This query not include reg_id for faster finding of the row. it is
    # not necessary, as we don't do spatial 'st_intersects' operation here, just
    # comparing 'subc_id' values, so that should be fast enough.
    # Furthermore, it wouldn't even be possible, as there is no way to retrieve
    # a subcatchment's reg_id from the "regional_units" table before doing this
    # query here.

    ### Define query:
    subc_ids = ','.join(map(str, subc_ids))
    columns = ','.join(columns)
    query = f'''
    SELECT {columns}
    FROM stream_segments
    WHERE subc_id = ANY(ARRAY[{subc_ids}])
    '''

    ### Get results as a dataframe:
    LOGGER.log(logging.TRACE, 'Querying database...')
    output_df = pd.read_sql_query(query, conn)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')
    return output_df



#################################
### for many points at a time ###
#################################

# TODO Deprecated, contains loop
# still used in: get_local_streamsegments_plural.py
# still used in: get_local_ids_plural.py
def get_subcid_basinid_regid_for_all_2json(conn, LOGGER, points_geojson, colname_site_id = None):
    # Input: GeoJSON
    # Output: JSON

    # When input is FeatureCollection, site_id is mandatory.

    # This function returns a weird statistic:
    #    output = {
    #    "subc_ids":   <comma-separated list>,
    #    "region_ids": <comma-separated list>,
    #    "basin_ids":  <comma-separated list>,
    #    "everything": { nested structure... }
    #}
    # TODO: Instead, return the same GeoJSON, but with added properties.

    # Create JSON object to be filled and returned:
    everything = {}
    # Create lists to be filled just for logging the results:
    basin_ids = []
    reg_ids = []
    subc_ids = []

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
            raise exc.UserInputException(err_msg)
        iterate_over = points_geojson['features']
        num = len(iterate_over)

    # Iterate over points and call "get_subcid_basinid_regid" for each point:
    # TODO: loop!! This is not super efficient, but the quickest to implement :)
    LOGGER.debug(f'Getting subcatchment for {num} lon, lat pairs...')
    LOGGER.warn("Using method 'get_subcid_basinid_regid_for_all_2json', which contains a loop.")

    for point in iterate_over: # either point or feature...

        # Get coordinates from input:
        if 'properties' in point:
            lon, lat = point['geometry']['coordinates']
        elif 'coordinates' in point:
            lon, lat = point['coordinates']
        else:
            err_msg = f"Input is not valid GeoJSON Point or Point-Feature: {point}"
            LOGGER.debug(err_msg)
            raise UserInputException(err_msg)

        # Query database:
        try:
            LOGGER.log(logging.TRACE, f'Getting subcatchment for lon, lat: {lon}, {lat}')
            subc_id, basin_id, reg_id = get_subcid_basinid_regid_from_lonlat(
                conn, LOGGER, lon, lat)
        except exc.GeoFreshNoResultException as e:
            # For example, if the point is in the ocean.
            # We return None. TODO: Test how this looks in JSON! (Note: Deprecated anyway)
            reg_id = basin_id = subc_id = None

        # Collect results in dict:
        # site_id is included by returning the points, so if the point includes a site_id, it is returned.
        s_reg_id = str(reg_id)
        s_basin_id = str(basin_id)
        s_subc_id = str(subc_id)

        if not s_reg_id in everything:
            everything[s_reg_id] = {s_basin_id: {s_subc_id: [point]}}
            # Note: Keys must be strings, otherwise pygeapi cannot store the structure into file...
        else:
            if not s_basin_id in everything[s_reg_id]:
                everything[s_reg_id][s_basin_id] = {s_subc_id: [point]}
            else:
                if not s_subc_id in everything[s_reg_id][s_basin_id]:
                    everything[s_reg_id][s_basin_id][s_subc_id] = [point]
                else:
                    everything[s_reg_id][s_basin_id][s_subc_id].append(point)

        # This is not really needed, just for logging:
        reg_ids.append(str(reg_id))
        basin_ids.append(str(basin_id))
        subc_ids.append(str(subc_id))

    # Finished collecting the results, now make output JSON object:
    # Note: This is not GeoJSON (on purpose), as we did not look for geometry yet.
    output = {
        "subc_ids": subc_ids,
        "region_ids": reg_ids,
        "basin_ids": basin_ids,
        "everything": everything
    }

    # Extensive logging of stats:
    LOGGER.log(logging.TRACE, f'Of {num} points, ...')

    # Stats reg_id...
    if len(set(reg_ids)) == 1:
        LOGGER.log(logging.TRACE, f'... all {num} points fall into regional unit with reg_id {reg_ids[0]}')
    else:
        reg_id_counts = {reg_id: reg_ids.count(reg_id) for reg_id in reg_ids}
        for reg_id in set(reg_ids):
            this_num = reg_id_counts[reg_id]
            LOGGER.log(logging.TRACE, f'... {this_num} points fall into regional unit with reg_id {reg_id}')

    if len(set(basin_ids)) == 1:
        LOGGER.log(logging.TRACE, f'... all {num} points fall into drainage basin with basin_id {basin_ids[0]}')
    else:
        basin_id_counts = {basin_id: basin_ids.count(basin_id) for basin_id in basin_ids}
        for basin_id in set(basin_ids):
            this_num = basin_id_counts[basin_id]
            LOGGER.log(logging.TRACE, f'... {this_num} points fall into drainage basin with basin_id {basin_id}')

    if len(set(subc_ids)) == 1:
        LOGGER.log(logging.TRACE, f'... all {num} points fall into subcatchment with subc_id {subc_ids[0]}')
    else:
        subc_id_counts = {subc_id: subc_ids.count(subc_id) for subc_id in subc_ids}
        for subc_id in set(subc_ids):
            this_num = subc_id_counts[subc_id]
            LOGGER.log(logging.TRACE, f'... {this_num} points fall into subcatchment with subc_id {subc_id}')

    # Return result
    return output


# TODO Deprecated, contains loop
# still used in: get_local_ids_plural.py
def get_basinid_regid_for_all_from_subcid_1csv(conn, LOGGER, input_df, colname_subc_id, colname_site_id):
    # Input: Pandas Dataframe
    # Output: Pandas Dataframe

    # Create list to be filled and converted to Pandas dataframe:
    everything = []
    site_id = None # in case none is provided.
    basin_ids = []
    reg_ids = []

    # Iterate over points and call "get_basinid_regid" for each point:
    # TODO: This is not super efficient, but the quickest to implement :)
    # TODO: Read this for alternatives to iteration: https://stackoverflow.com/questions/16476924/how-can-i-iterate-over-rows-in-a-pandas-dataframe
    num = input_df.shape[0]
    LOGGER.debug(f'Getting basin_id, reg_id for {num} subc_ids...')

    # Retrieve using column index, not colname - this is faster:
    colidx_subc_id = input_df.columns.get_loc(colname_subc_id)
    colidx_site_id = input_df.columns.get_loc(colname_site_id)

    # Iterate over rows:
    LOGGER.warn("Using method 'get_basinid_regid_for_all_from_subcid_1csv', which contains a loop.")
    for row in input_df.itertuples(index=False):

        # Get coordinates from input:
        subc_id = row[colidx_subc_id]
        site_id = row[colidx_site_id]

        # Query database:
        try:
            LOGGER.log(logging.TRACE, f'Getting basin_id, reg_id for subc_id {subc_id}')
            basin_id, reg_id = get_basinid_regid_from_subcid(
                conn, LOGGER, subc_id)
        except exc.GeoFreshNoResultException as e:
            # For example, if the point is in the ocean.
            # In Pandas dataframe, NaN is returned.
            reg_id = basin_id = None

        # Collect results in list:
        everything.append([site_id, subc_id, reg_id, basin_id])

    # Finished collecting the results, now make pandas dataframe:
    output_df = pd.DataFrame(everything, columns=['site_id', 'subc_id', 'reg_id', 'basin_id'])
    return output_df


#######################
### For testing ... ###
#######################

if __name__ == "__main__":

    try:
        # If the package is properly installed, thus it is findable by python on PATH:
        import aqua90m.utils.extent_helpers as extent_helpers
        import aqua90m.utils.geojson_helpers as geojson_helpers
        import aqua90m.utils.exceptions as exc
        import aqua90m.geofresh.temp_table_for_queries as temp_tables
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.utils.extent_helpers as extent_helpers
        import aqua90m.utils.geojson_helpers as geojson_helpers
        import aqua90m.utils.exceptions as exc
        import aqua90m.geofresh.temp_table_for_queries as temp_tables


    # Logging
    verbose = True
    #logging.TRACE = 5
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    #logging.basicConfig(level=logging.TRACE, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
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


#############################
### Run function singular ###
#############################

if __name__ == "__main__" and True:

    print('\nSTART RUNNING FUNCTION: get_regid')
    res = get_regid(conn, LOGGER, 9.931555, 54.695070)
    print(f'RESULT: {res}')
    res = get_regid(conn, LOGGER, subc_id=506250459)
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_basinid_regid')
    basin_id, reg_id = get_basinid_regid(conn, LOGGER, 9.931555, 54.695070)
    print(f'RESULT: {basin_id} {reg_id}')
    basin_id, reg_id = get_basinid_regid(conn, LOGGER, subc_id=506250459)
    print(f'RESULT: {basin_id} {reg_id}')

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid')
    res = get_subcid_basinid_regid(conn, LOGGER, 9.931555, 54.695070)
    print(f'RESULT: {res[0]} {res[1]} {res[2]}')
    res = get_subcid_basinid_regid(conn, LOGGER, subc_id=506250459)
    print(f'RESULT: {res[0]} {res[1]} {res[2]}')

    print('\nSTART RUNNING HELPER FUNCTION: get_subcid_basinid')
    res = get_subcid_basinid_from_lonlat_regid(conn, LOGGER, 9.931555, 54.695070, 58)
    print(f'RESULT: {res[0]} {res[1]}')

    print('\nSTART RUNNING FUNCTION: get_regid_from_basinid')
    res = get_regid_from_basinid(conn, LOGGER, 1288419)
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_all_subcids_from_basinid')
    res = get_all_subcids_from_basinid(conn, LOGGER, 1288419, 58, min_strahler=None)
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_all_subcids_from_basinid (min_strahler=4)')
    res = get_all_subcids_from_basinid(conn, LOGGER, 1288419, 58, min_strahler=4)
    print(f'RESULT: {res}')


###########################
### Run function plural ###
### Input Dataframe     ###
###########################

if __name__ == "__main__" and True:

    example_df = pd.DataFrame(
        [
            ['aa', 10.041155219078064, 53.07006147583069],
            ['bb', 10.042726993560791, 53.06911450500803],
            ['cc', 10.039894580841064, 53.06869677412868],
            ['a',  10.698832912677716, 53.51710727672125],
            ['b',  12.80898022975407,  52.42187129944509],
            ['c',  11.915323076217902, 52.730867141970464],
            ['d',  16.651903948708565, 48.27779486850176],
            ['e',  19.201146608148463, 47.12192880511424],
            ['f',  24.432498016999062, 61.215505889934434],
            ['sea',  8.090485, 54.119322]
        ], columns=['my_site', 'lon', 'lat']
    )

    print('\nSTART RUNNING FUNCTION: get_regid__dataframe_to_dataframe')
    res = get_regid__dataframe_to_dataframe(conn, example_df, "lon", "lat", "my_site")
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid__dataframe_to_dataframe')
    res = get_subcid_basinid_regid__dataframe_to_dataframe(conn, example_df, "lon", "lat", "my_site")
    print(f'RESULT: {res}')

    ### Special case: Using subc_ids as input:
    subc_ids = [506853766,506853766,506853766,506601172,507307015,507081236,90929627,91875954,553374842]

    print('\nSTART RUNNING FUNCTION: get_basinid_regid_from_subcid_plural')
    res = get_basinid_regid_from_subcid_plural(conn, subc_ids, columns=['subc_id', 'basin_id', 'reg_id'])
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_basinid_regid_from_subcid_plural')
    res = get_basinid_regid_from_subcid_plural(conn, subc_ids, columns=['reg_id'])
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_basinid_regid_from_subcid_plural')
    res = get_basinid_regid_from_subcid_plural(conn, subc_ids, columns=['reg_id', 'basin_id'])
    print(f'RESULT: {res}')


###########################
### Run function plural ###
### Input GeoJSON       ###
###########################

if __name__ == "__main__" and True:

    example_fcoll = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"my_site": 'aa'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [10.041155219078064, 53.07006147583069]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'bb'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [10.042726993560791, 53.06911450500803]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'cc'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [10.039894580841064, 53.06869677412868]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'a'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [10.698832912677716, 53.51710727672125]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'b'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [12.80898022975407,  52.42187129944509]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'c'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [11.915323076217902, 52.730867141970464]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'd'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [16.651903948708565, 48.27779486850176]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'e'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [19.201146608148463, 47.12192880511424]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'f'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [24.432498016999062, 61.215505889934434]
                }
            },
            {
                "type": "Feature",
                "properties": {"my_site": 'sea'},
                "geometry": {
                    "type": "Point",
                    "coordinates": [8.090485, 54.119322]
                }
            }
        ]
    }

    example_gcoll = {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "Point",
                "coordinates": [10.041155219078064, 53.07006147583069]
            },
            {
                "type": "Point",
                "coordinates": [10.042726993560791, 53.06911450500803]
            },
            {
                "type": "Point",
                "coordinates": [10.039894580841064, 53.06869677412868]
            },
            {
                "type": "Point",
                "coordinates": [10.698832912677716, 53.51710727672125]
            },
            {
                "type": "Point",
                "coordinates": [12.80898022975407,  52.42187129944509]
            },
            {
                "type": "Point",
                "coordinates": [11.915323076217902, 52.730867141970464]
            },
            {
                "type": "Point",
                "coordinates": [16.651903948708565, 48.27779486850176]
            },
            {
                "type": "Point",
                "coordinates": [19.201146608148463, 47.12192880511424]
            },
            {
                "type": "Point",
                "coordinates": [24.432498016999062, 61.215505889934434]
            },
            {
                "type": "Point",
                "coordinates": [8.090485, 54.119322]
            }
        ]
    }


    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid__geojson_to_dataframe')
    res = get_subcid_basinid_regid__geojson_to_dataframe(conn, example_fcoll)
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid__geojson_to_dataframe')
    res = get_subcid_basinid_regid__geojson_to_dataframe(conn, example_gcoll)
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_regid__geojson_to_dataframe')
    res = get_regid__geojson_to_dataframe(conn, example_fcoll)
    print(f'RESULT: {res}')

    print('\nSTART RUNNING FUNCTION: get_regid__geojson_to_dataframe')
    res = get_regid__geojson_to_dataframe(conn, example_gcoll)
    print(f'RESULT: {res}')


###########################
### Run function plural ###
### Input GeoJSON       ###
### DEPRECATED          ###
###########################

if __name__ == "__main__" and True:

    points_geojson = {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "Point",
                "coordinates": [ 10.698832912677716, 53.51710727672125 ]
            },
            {
                "type": "Point",
                "coordinates": [ 12.80898022975407, 52.42187129944509 ]
            },
            {
                "type": "Point",
                "coordinates": [ 11.915323076217902, 52.730867141970464 ]
            },
            {
                "type": "Point",
                "coordinates": [ 16.651903948708565, 48.27779486850176 ]
            },
            {
                "type": "Point",
                "coordinates": [ 19.201146608148463, 47.12192880511424 ]
            },
            {
                "type": "Point",
                "coordinates": [ 24.432498016999062, 61.215505889934434 ]
            }
        ]
    }

    # All three points in same reg_id (58), basin_id (1294020) and subc_id (506853766)
    points_geojson_all_same = {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "Point",
                "coordinates": [ 10.041155219078064, 53.07006147583069 ]
            },
            {
                "type": "Point",
                "coordinates": [ 10.042726993560791, 53.06911450500803 ]
            },
            {
                "type": "Point",
                "coordinates": [ 10.039894580841064, 53.06869677412868 ]
            }
        ]
    }

    points_geojson_with_siteid = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ 10.698832912677716, 53.51710727672125 ]
                },
                "properties": { "site_id": "a" }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ 12.80898022975407, 52.42187129944509 ]
                },
                "properties": { "site_id": "b" }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ 11.915323076217902, 52.730867141970464 ]
                },
                "properties": { "site_id": "c" }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ 16.651903948708565, 48.27779486850176 ]
                },
                "properties": { "site_id": "d" }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ 19.201146608148463, 47.12192880511424 ]
                },
                "properties": { "site_id": "e" }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ 24.432498016999062, 61.215505889934434 ]
                },
                "properties": { "site_id": "f" }
            }
        ]
    }

    # Input: GeoJSON, output JSON
    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_2json (using Multipoint)')
    res = get_subcid_basinid_regid_for_all_2json(conn, LOGGER, points_geojson)
    print(f'RESULT:\n{res}')

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_2json (with site_id)')
    res = get_subcid_basinid_regid_for_all_2json(conn, LOGGER, points_geojson_with_siteid, "site_id")
    print(f'RESULT:\n{res}')


    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_2json (with site_id, but omit it...)')
    try:
        res = get_subcid_basinid_regid_for_all_2json(conn, LOGGER, points_geojson_with_siteid)
        print('ERROR! Should not have worked!')
        import sys
        sys.exit(1)
    except Exception as e:
        print(f'RESULT:\nException was raised as desired: {e}')


    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_2json (all in same region)')
    res = get_subcid_basinid_regid_for_all_2json(conn, LOGGER, points_geojson_all_same)
    print(f'RESULT:\n{res}')


###########################
### Run function plural ###
### Input Dataframe     ###
### DEPRECATED          ###
###########################

if __name__ == "__main__" and True:

    ## Input: dataframe, output dataframe, with site_id!
    example_dataframe2 = pd.DataFrame(
        [
            ['aa', 506853766],
            ['bb', 506853766],
            ['cc', 506853766],
            ['a', 506601172],
            ['b', 507307015],
            ['c', 507081236],
            ['d', 90929627],
            ['e', 91875954],
            ['f', 553374842],
        ], columns=['site_id', 'subc_id']
    )

    print('\nSTART RUNNING FUNCTION: get_basinid_regid_for_all_from_subcid_1csv (input: dataframe, output: dataframe)')
    res = get_basinid_regid_for_all_from_subcid_1csv(conn, LOGGER, example_dataframe2, 'subc_id', 'site_id')
    print(f'RESULT:\n{res}')


###################
### Finally ... ###
###################

if __name__ == "__main__":
    conn.close()
