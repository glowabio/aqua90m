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
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.extent_helpers as extent_helpers
        import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
        import pygeoapi.process.aqua90m.utils.exceptions as exc
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+'. If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)


def get_regid(conn, lon, lat):

    extent_helpers.check_outside_europe(lon, lat)
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
    query = """
    SELECT reg_id
    FROM regional_units
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), geom)
    """.format(longitude = lon, latitude = lat)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None: # Ocean case:
        err_msg      = 'No region id found for lon %s, lat %s! Is this in the ocean?' % (lon, lat)
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)

    else:
        reg_id = row[0]

    return reg_id


def get_subcid_basinid(conn, lon, lat, reg_id):

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

    query = """
    SELECT
    subc_id,
    basin_id
    FROM sub_catchments
    WHERE st_intersects(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326), geom)
    AND reg_id = {reg_id}
    """.format(longitude = lon, latitude = lat, reg_id = reg_id)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results:
    row = cursor.fetchone()
    if row is None: # Ocean case:
        err_msg = 'No subc_id and basin_id found for lon, lat %s, %s. Is this located in the ocean?' % (lon, lat)
        LOGGER.error(err_msg)
        raise exc.GeoFreshNoResultException(err_msg)
    else:
        subc_id = row[0]
        basin_id = row[1]

    return subc_id, basin_id 


def get_basinid_regid(conn, subc_id):
    # TODO: We need this in plural for geofresh.get_env90m_data_for_subcids.py

    ### Define query:
    query = """
    SELECT basin_id, reg_id
    FROM sub_catchments
    WHERE subc_id = {given_subc_id}
    """.format(given_subc_id = subc_id)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None:
        err_msg = 'No basin_id and reg_id found for subc_id %s!' % subc_id
        LOGGER.error(err_msg)
        raise exc.GeoFreshUnexpectedResultException(error_message)
    else:
        basin_id = row[0]
        reg_id = row[1]

    return basin_id, reg_id


def get_subcid_basinid_regid(conn, LOGGER, lon = None, lat = None, subc_id = None):

    # Non-standard case: If user provided subc_id, then use it!
    if subc_id is not None:
        LOGGER.log(logging.TRACE, 'Getting subcatchment, region and basin id for subc_id: %s' % subc_id)
        basin_id, reg_id = get_basinid_regid(conn, subc_id)

    # Standard case: User provided lon and lat!
    elif lon is not None and lat is not None:
        LOGGER.log(logging.TRACE, 'Getting subcatchment, region and basin id for lon, lat: %s, %s' % (lon, lat))
        lon = float(lon)
        lat = float(lat)
        reg_id = get_regid(conn, lon, lat)
        subc_id, basin_id = get_subcid_basinid(conn, lon, lat, reg_id)

    else:
        err_msg = 'Lon and lat (or subc_id) have to be provided! Lon: %s, lat: %s, subc_id %s' % (lon, lat, subc_id)
        raise UserInputException(err_msg)

    LOGGER.log(logging.TRACE, 'Subcatchment has subc_id %s, basin_id %s, reg_id %s.' % (subc_id, basin_id, reg_id))
    return subc_id, basin_id, reg_id


#################################
### for many points at a time ###
#################################

def get_subcid_basinid_regid_for_all_1(conn, LOGGER, points_geojson, colname_site_id = None):
    # Input: GeoJSON
    # Output: JSON

    # TODO: When input is FeatureCollection, make site_id mandatory?
    #
    # This returns a weird statistic:
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
    # TODO: This is not super efficient, but the quickest to implement :)
    LOGGER.debug('Getting subcatchment for %s lon, lat pairs...' % num)
    for point in iterate_over: # either point or feature...

        # Get coordinates from input:
        if 'properties' in point:
            lon, lat = point['geometry']['coordinates']
        elif 'coordinates' in point:
            lon, lat = point['coordinates']
        else:
            err_msg = "Input is not valid GeoJSON Point or Point-Feature: %s" % point
            raise UserInputException(err_msg)

        # Query database:
        try:
            LOGGER.log(logging.TRACE, 'Getting subcatchment for lon, lat: %s, %s' % (lon, lat))
            subc_id, basin_id, reg_id = get_subcid_basinid_regid(
                conn, LOGGER, lon, lat, None)
        except exc.GeoFreshNoResultException as e:
            # For example, if the point is in the ocean.
            # We return None. TODO: Test how this looks in JSON!
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
        "subc_ids":   ', '.join(str(i) for i in set(subc_ids)),
        "region_ids": ', '.join(str(i) for i in set(reg_ids)),
        "basin_ids":  ', '.join(str(i) for i in set(basin_ids)),
        "everything": everything
    }

    # Extensive logging of stats:
    LOGGER.log(logging.TRACE, 'Of %s points, ...' % num)

    # Stats reg_id...
    if len(set(reg_ids)) == 1:
        LOGGER.log(logging.TRACE, '... all %s points fall into regional unit with reg_id %s' % (num, reg_ids[0]))
    else:
        reg_id_counts = {reg_id: reg_ids.count(reg_id) for reg_id in reg_ids}
        for reg_id in set(reg_ids):
            LOGGER.log(logging.TRACE, '... %s points fall into regional unit with reg_id %s' % (reg_id_counts[reg_id], reg_id))

    if len(set(basin_ids)) == 1:
        LOGGER.log(logging.TRACE, '... all %s points fall into drainage basin with basin_id %s' % (num, basin_ids[0]))
    else:
        basin_id_counts = {basin_id: basin_ids.count(basin_id) for basin_id in basin_ids}
        for basin_id in set(basin_ids):
            LOGGER.log(logging.TRACE, '... %s points fall into drainage basin with basin_id %s' % (basin_id_counts[basin_id], basin_id))

    if len(set(subc_ids)) == 1:
        LOGGER.log(logging.TRACE, '... all %s points fall into subcatchment with subc_id %s' % (num, subc_ids[0]))
    else:
        subc_id_counts = {subc_id: subc_ids.count(subc_id) for subc_id in subc_ids}
        for subc_id in set(subc_ids):
            LOGGER.log(logging.TRACE, '... %s points fall into subcatchment with subc_id %s' % (subc_id_counts[subc_id], subc_id))

    # Return result
    return output




def get_subcid_basinid_regid_for_all_2(conn, LOGGER, input_dataframe, colname_lon, colname_lat, colname_site_id):
    # Input: Pandas Dataframe
    # Output: Pandas Dataframe

    # Create list to be filled and converted to Pandas dataframe:
    everything = []
    site_id = None # in case none is provided.
    basin_ids = []
    reg_ids = []
    subc_ids = []

    # Iterate over points and call "get_subcid_basinid_regid" for each point:
    # TODO: This is not super efficient, but the quickest to implement :)
    # TODO: Read this for alternatives to iteration: https://stackoverflow.com/questions/16476924/how-can-i-iterate-over-rows-in-a-pandas-dataframe
    num = input_dataframe.shape[0]
    LOGGER.debug('Getting subcatchment for %s lon, lat pairs...' % num)

    for row in input_dataframe.itertuples(index=False):

        # Get coordinates from input:
        #lon = row.lon
        #lat = row.lat
        #site_id = row.site_id
        # TODO OPTIMIZE: getattr may not be the fastest way of accessing this...
        lon = getattr(row, colname_lon)
        lat = getattr(row, colname_lat)
        site_id = getattr(row, colname_site_id)

        # Query database:
        try:
            LOGGER.log(logging.TRACE, 'Getting subcatchment for lon, lat: %s, %s' % (lon, lat))
            subc_id, basin_id, reg_id = get_subcid_basinid_regid(
                conn, LOGGER, lon, lat, None)
        except exc.GeoFreshNoResultException as e:
            # For example, if the point is in the ocean.
            # We return None. TODO: Test how this looks in Pandas dataframe!
            reg_id = basin_id = subc_id = None

        # Collect results in list:
        everything.append([site_id, reg_id, basin_id, subc_id])

        # This is not really needed, just for logging:
        reg_ids.append(str(reg_id))
        basin_ids.append(str(basin_id))
        subc_ids.append(str(subc_id))

    # Finished collecting the results, now make pandas dataframe:
    dataframe = pd.DataFrame(everything, columns=['site_id', 'reg_id', 'basin_id', 'subc_id'])

    # Extensive logging of stats:
    LOGGER.log(logging.TRACE, 'Of %s points, ...' % num)

    if len(set(reg_ids)) == 1:
        LOGGER.log(logging.TRACE, '... all %s points fall into regional unit with reg_id %s' % (num, reg_ids[0]))
    else:
        reg_id_counts = {reg_id: reg_ids.count(reg_id) for reg_id in reg_ids}
        for reg_id in set(reg_ids):
            LOGGER.log(logging.TRACE, '... %s points fall into regional unit with reg_id %s' % (reg_id_counts[reg_id], reg_id))

    if len(set(basin_ids)) == 1:
        LOGGER.log(logging.TRACE, '... all %s points fall into drainage basin with basin_id %s' % (num, basin_ids[0]))
    else:
        basin_id_counts = {basin_id: basin_ids.count(basin_id) for basin_id in basin_ids}
        for basin_id in set(basin_ids):
            LOGGER.log(logging.TRACE, '... %s points fall into drainage basin with basin_id %s' % (basin_id_counts[basin_id], basin_id))

    if len(set(subc_ids)) == 1:
        LOGGER.log(logging.TRACE, '... all %s points fall into subcatchment with subc_id %s' % (num, subc_ids[0]))
    else:
        subc_id_counts = {subc_id: subc_ids.count(subc_id) for subc_id in subc_ids}
        for subc_id in set(subc_ids):
            LOGGER.log(logging.TRACE, '... %s points fall into subcatchment with subc_id %s' % (subc_id_counts[subc_id], subc_id))

    # Return result
    return dataframe


if __name__ == "__main__":

    try:
        # If the package is properly installed, thus it is findable by python on PATH:
        import aqua90m.utils.extent_helpers as extent_helpers
        import aqua90m.utils.geojson_helpers as geojson_helpers
        import aqua90m.utils.exceptions as exc
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.utils.extent_helpers as extent_helpers
        import aqua90m.utils.geojson_helpers as geojson_helpers
        import aqua90m.utils.exceptions as exc


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

    print('\nSTART RUNNING FUNCTION: get_regid')
    lon = 9.931555
    lat = 54.695070
    res = get_regid(conn, lon, lat)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid')
    lon = 9.931555
    lat = 54.695070
    reg_id = 58
    res = get_subcid_basinid(conn, lon, lat, reg_id)
    print('RESULT: %s %s' % (res[0], res[1]))

    print('\nSTART RUNNING FUNCTION: get_basinid_regid')
    one_subc_id = 506250459
    res = get_basinid_regid(conn, one_subc_id)
    print('RESULT: %s %s' % (res[0], res[1]))

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid (using subc_id)')
    one_subc_id = 506250459
    res = get_subcid_basinid_regid(conn, LOGGER, lon = None, lat = None, subc_id = one_subc_id)
    print('RESULT: %s %s %s' % (res[0], res[1], res[2]))

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid (using lon, lat)')
    lon = 9.931555
    lat = 54.695070
    res = get_subcid_basinid_regid(conn, LOGGER, lon = lon, lat = lat, subc_id = None)
    print('RESULT: %s %s %s' % (res[0], res[1], res[2]))

    ###########################
    ### Run function plural ###
    ###########################

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
    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_1 (using Multipoint)')
    res = get_subcid_basinid_regid_for_all_1(conn, LOGGER, points_geojson)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_1 (with site_id)')
    res = get_subcid_basinid_regid_for_all_1(conn, LOGGER, points_geojson_with_siteid, "site_id")
    print('RESULT:\n%s' % res)


    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_1 (with site_id, but omit it...)')
    try:
        res = get_subcid_basinid_regid_for_all_1(conn, LOGGER, points_geojson_with_siteid)
        print('ERROR! Should not have worked!')
        import sys
        sys.exit(1)
    except Exception as e:
        print('RESULT:\nException was raised as desired: %s' % e)


    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_1 (all in same region)')
    res = get_subcid_basinid_regid_for_all_1(conn, LOGGER, points_geojson_all_same)
    print('RESULT:\n%s' % res)

    ## Input: dataframe, output dataframe, with site_id!
    print('\nSTART RUNNING FUNCTION: get_subcid_basinid_regid_for_all_2 (input: dataframe, output: dataframe)')
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
        ], columns=['site_id', 'lon', 'lat']
    )
    res = get_subcid_basinid_regid_for_all_2(conn, LOGGER, example_dataframe, 'lon', 'lat', 'site_id')
    print('RESULT:\n%s' % res)
