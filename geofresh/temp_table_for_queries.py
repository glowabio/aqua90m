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


def populate_temp_table(cursor, tablename, list_of_insert_rows):
    LOGGER.debug(f'Populating temp table "{tablename}"...')

    # Inserting the information passed by the user:
    _fill_temp_table(cursor, tablename, list_of_insert_rows)

    # Generate a spatial index:
    _add_index(cursor, tablename)

    # For each point, find out and store and retrieve the reg_id:
    reg_ids = _update_temp_table_regid(cursor, tablename)

    # For each point, find out and store the basin_id and subc_id:
    _add_subcids(cursor, tablename, reg_ids)

    LOGGER.debug(f'Populating temp table "{tablename}"... done.')
    return reg_ids


def _tablename(tablename_prefix):
    randomstring = str(uuid.uuid4()).replace('-', '')
    return f'{tablename_prefix}_{randomstring}'


def drop_temp_table(cursor, tablename):
    LOGGER.debug(f'Dropping temporary table "{tablename}"...')
    query = f"DROP TABLE IF EXISTS {tablename};"
    cursor.execute(query)
    LOGGER.debug(f'Dropping temporary table "{tablename}"... done.')


def make_insertion_rows_from_geojson(geojson, colname_site_id=None):
    list_of_insert_rows = []
    # TODO: How to deal with missing site_ids? Maybe fill with NULL values, or
    # not create that column if it is not needed?

    if geojson['type'] == 'MultiPoint':
        LOGGER.debug('Found MultiPoint...')
        site_id = 'none'
        for lon, lat in geojson["coordinates"]:
            row = f"('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))"
            list_of_insert_rows.append(row)

    elif geojson['type'] == 'GeometryCollection':
        LOGGER.debug('Found GeometryCollection...')
        site_id = 'none'
        for point in geojson['geometries']:
            lon, lat = point['coordinates']
            row = f"('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))"
            list_of_insert_rows.append(row)

    elif geojson['type'] == 'FeatureCollection':
        LOGGER.debug('Found FeatureCollection...')
        for point in geojson['features']:
            lon, lat = point['geometry']['coordinates']
            site_id = point['properties'][colname_site_id]
            row = f"('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))"
            list_of_insert_rows.append(row)

    else:
        err_msg = 'Cannot recognize GeoJSON object!'
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)

    LOGGER.debug(f'Created list of {len(list_of_insert_rows)} insert rows...')
    LOGGER.debug(f'First insert rows:\n{list_of_insert_rows[0]}\n{list_of_insert_rows[1]}')
    return list_of_insert_rows


def make_insertion_rows_from_dataframe(dataframe, colname_lon, colname_lat, colname_site_id):
    list_of_insert_rows = []
    for row in dataframe.itertuples(index=False):
        lon = getattr(row, colname_lon)
        lat = getattr(row, colname_lat)
        site_id = getattr(row, colname_site_id)
        row = f"('{site_id}', {lon}, {lat}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))"
        list_of_insert_rows.append(row)

    LOGGER.debug(f'Created list of {len(list_of_insert_rows)} insert rows...')
    LOGGER.debug(f'First insert row:\n{list_of_insert_rows[0]}')
    return list_of_insert_rows


def create_temp_table(cursor, tablename_prefix):
    tablename =_tablename(tablename_prefix)
    LOGGER.debug(f'Creating temporary table "{tablename}"...')
    # TODO WIP numeric or decimal or ...?
    # TODO: Is varchar a good type for expected site_ids?
    query = f"""
    CREATE TEMP TABLE {tablename} (
    site_id varchar(100),
    lon decimal,
    lat decimal,
    subc_id integer,
    basin_id integer,
    reg_id smallint,
    geom_user geometry(POINT, 4326)
    );
    """
    query = query.replace("\n", " ")
    _start = time.time()
    cursor.execute(query)
    _end = time.time()
    LOGGER.debug(f'Creating temporary table "{tablename}"... done.')
    LOGGER.log(logging.TRACE, '**** TIME ************ query_create: %s' % (_end - _start))
    return tablename


def create_temp_table_for_strahler_snapping(cursor, tablename_prefix):
    # TODO: Should this be placed in snapping_strahler.py?
    tablename =_tablename(tablename_prefix)
    LOGGER.debug(f'Creating temporary table "{tablename}" (with column "geom_closest")...')
    # TODO WIP numeric or decimal or ...?
    # TODO: Is varchar a good type for expected site_ids?
    query = f"""
    CREATE TEMP TABLE {tablename} (
    site_id varchar(100),
    lon decimal,
    lat decimal,
    subc_id integer,
    basin_id integer,
    reg_id smallint,
    geom_user geometry(POINT, 4326),
    geom_closest geometry(LINESTRING, 4326),
    subcid_closest integer,
    strahler_closest integer
    );
    """
    query = query.replace("\n", " ")
    _start = time.time()
    cursor.execute(query)
    _end = time.time()
    LOGGER.debug(f'Creating temporary table "{tablename}" (with column "geom_closest")... done.')
    LOGGER.log(logging.TRACE, '**** TIME ************ query: %s' % (_end - _start))
    return tablename


def _fill_temp_table(cursor, tablename, list_of_insert_rows):
    LOGGER.debug(f'Inserting into temporary table "{tablename}"...')
    list_of_insertions = ", ".join(list_of_insert_rows)
    query = f'INSERT INTO {tablename} (site_id, lon, lat, geom_user) VALUES {list_of_insertions};'
    _start = time.time()
    cursor.execute(query)
    _end = time.time()
    LOGGER.debug(f'Inserting into temporary table "{tablename}"... done.')
    LOGGER.log(logging.TRACE, '**** TIME ************ query_insert: %s' % (_end - _start))


def _add_index(cursor, tablename):
    LOGGER.debug(f'Creating index for temporary table "{tablename}"...')
    query = f'CREATE INDEX IF NOT EXISTS temp_test_geom_user_idx ON {tablename} USING gist (geom_user);'
    _start = time.time()
    cursor.execute(query)
    _end = time.time()
    LOGGER.debug(f'Creating index for temporary table "{tablename}"... done.')
    LOGGER.log(logging.TRACE, '**** TIME ************ query_index: %s' % (_end - _start))


def _update_temp_table_regid(cursor, tablename):

    ## Add reg_id to temp table, get it returned:
    LOGGER.debug(f'Update reg_id (st_intersects) in temporary table "{tablename}"...')
    query = f"""
        WITH updater AS (
            UPDATE {tablename}
            SET reg_id = reg.reg_id
            FROM regional_units reg
            WHERE st_intersects({tablename}.geom_user, reg.geom)
            RETURNING {tablename}.reg_id
        )
        SELECT DISTINCT reg_id FROM updater;
    """
    query = query.replace("\n", " ")
    _start = time.time()
    cursor.execute(query)
    _end = time.time()
    LOGGER.debug(f'Update reg_id (st_intersects) in temporary table "{tablename}"... done')
    LOGGER.log(logging.TRACE, '**** TIME ************ query_reg: %s' % (_end - _start))

    ## Retrieve reg_id, for next query:
    LOGGER.log(logging.TRACE, 'Retrieving reg_ids (RETURNING from UPDATE query)...')
    reg_id_set = set()
    while (True):
        row = cursor.fetchone()
        if row is None: break
        reg_id = row[0]
        LOGGER.log(logging.TRACE, f'  Retrieved: {reg_id}')
        reg_id_set.add(reg_id)
    LOGGER.debug(f'Set of reg_ids: {reg_id_set}')
    return reg_id_set


def _add_subcids(cursor, tablename, reg_ids):

    LOGGER.debug(f'Update subc_id, basin_id (st_intersects) in temporary table "{tablename}"...')
    reg_ids_string = ", ".join([str(elem) for elem in reg_ids])
    query = f"""
        UPDATE {tablename}
        SET subc_id = sub.subc_id, basin_id = sub.basin_id
        FROM sub_catchments sub
        WHERE st_intersects({tablename}.geom_user, sub.geom) AND sub.reg_id IN ({reg_ids_string});
    """
    _start = time.time()
    cursor.execute(query)
    _end = time.time()
    LOGGER.log(logging.TRACE, '**** TIME ************ query_sub_bas: %s' % (_end - _start))
    LOGGER.debug(f'Update subc_id, basin_id (st_intersects) in temporary table "{tablename}"... done.')




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
        import aqua90m.utils.exceptions as exc
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
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


    ####################################
    ### Run function for many points ###
    ### input GeoJSON                ###
    ####################################

    input_geojson_multipoint1 = {
        "type": "MultiPoint",
        "coordinates": [
            [9.931555, 54.695070],
            [9.921555, 54.295070]
        ]
    }

    input_geojson_multipoint2 = {
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

    input_geojson_geomcoll1 = {
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

    input_geojson_featurecoll1 = {
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


    print('\nSTART RUNNING FUNCTION: make_insertion_rows_from_geojson, FeatureCollection...')
    rows = make_insertion_rows_from_geojson(input_geojson_multipoint1, "my_site")
    print('RESULT: %s' % rows)

    print('\nSTART RUNNING FUNCTION: make_insertion_rows_from_geojson, FeatureCollection...')
    rows = make_insertion_rows_from_geojson(input_geojson_featurecoll1, "my_site")
    print('RESULT: %s' % rows)

    print('\nSTART RUNNING FUNCTION: make_insertion_rows_from_geojson, GeometryCollection...')
    rows = make_insertion_rows_from_geojson(input_geojson_featurecoll1, "my_site")
    print('RESULT: %s' % rows)

    print('\nSTART RUNNING FUNCTION: make_insertion_rows_from_dataframe, data_frame...')
    rows = make_insertion_rows_from_dataframe(example_dataframe, 'lon', 'lat', 'my_site')
    print('RESULT: %s' % rows)


    print('\nSTART RUNNING FUNCTION: create_and_fill_temp_table')
    cursor = conn.cursor()
    start = time.time()
    create_and_fill_temp_table(cursor, rows, 'test1')
    end = time.time()
    print('TIME: %s' % (end - start))
    drop_temp_table(cursor, tablename)


    print('\nTEST CUSTOM EXCEPTION: make_insertion_rows_from_geojson, FeatureCollection...')
    try:
        res = make_insertion_rows_from_geojson(conn, input_geojson_featurecoll1)
        raise RuntimeError('Should not reach here!')
    except exc.UserInputException as e:
        print('RESULT: Proper exception, saying: %s' % e)
