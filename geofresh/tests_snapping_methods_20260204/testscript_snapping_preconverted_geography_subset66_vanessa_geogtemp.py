import logging
import time
import json
import pandas as pd
from io import StringIO
from datetime import datetime
import uuid


from temp_table_for_queries import make_insertion_rows_from_dataframe
# NOT IMPORTING THIS ANYMORE, but redefine it locally here with changed column types!
#from temp_table_for_queries import create_and_populate_temp_table
from temp_table_for_queries import drop_temp_table
from temp_table_for_queries import log_query_time
from snapping_strahler import _package_result_in_dataframe
from database_connection import get_connection_object_config

#LOGGER = logging.getLogger(__name__)
LOGGER = logging.getLogger('testscript')


### Nearest neighbour query
query_nearest_with_geography_VB = '''
    UPDATE {tablename} AS temp1
    SET
        geog_closest = closest.geog,
        strahler_closest = closest.strahler,
        subcid_closest = closest.subc_id
    FROM {tablename} AS temp2
    CROSS JOIN LATERAL (
        SELECT seg.geog, seg.strahler, seg.subc_id
        FROM "shiny_user"."stream_segments_geog_66" seg
        WHERE seg.geog IS NOT NULL AND seg.strahler >= {min_strahler}
        ORDER BY seg.geog <-> temp2.geog_user
        LIMIT 1
    ) AS closest
    WHERE temp1.geog_user = temp2.geog_user;
'''

### THIS IS A COPY OF testscript_snapping_preconverted_geography_subset66_vanessa.py
### BUT I MODIFIED THIS TO CHANGE THE TEMP TABLE COLUMNS
### FROM GEOMETRY TO GEOGRAPHY
### TO SEE WHAT THAT CHANGES!!

#if __name__ == '__main__':
def main():

    #csv_url_or_path = 'https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus_with_basinid.csv'
    #csv_url_or_path = '/var/www/nginx/referencedata/aqua90m/spdata_barbus_with_basinid.csv'
    csv_url_or_path = 'https://aqua.igb-berlin.de/referencedata/aqua90m/fish_all_species_snapped_removed_empties.csv'
    csv_url_or_path = '/var/www/nginx/referencedata/aqua90m/fish_all_species_snapped_removed_empties.csv'
    #csv_url_or_path = 'https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus_with_basinid_2points.csv'
    #csv_url_or_path = '/var/www/nginx/referencedata/aqua90m/spdata_barbus_with_basinid_2points.csv'
    #csv_url_or_path = None # so use two example points


    #colname_lon = 'lon'
    #colname_lat = 'lat'
    colname_lon = 'longitude_original'
    colname_lat = 'latitude_original'
    colname_site_id = 'site_id'
    add_distance = True
    min_strahler = 4
    config_file_path = "/opt/pyg_upstream_dev/pygeoapi/config.geofreshprod.json"
    output_csv_name = None # defined later based on query

    ###################
    ### Preparation ###
    ###################

    # Logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s:%(lineno)s - %(levelname)5s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # which query
    now = datetime.now()
    query_nearest_neighbours = query_nearest_with_geography_VB
    queryname = 'query_nearest_with_geography_VB'
    output_csv_name = f'test_{queryname}_{now:%Y%m%d_%H-%M-%S}.csv'
    LOGGER.info(f'Testing query: {queryname}, starting at {now:%Y%m%d_%H-%M-%S}')

    # Read CSV file to Pandas Dataframe
    if csv_url_or_path is None:
            csv_string = """site_id,lon,lat,subc_id,mybasin,reg_id
FP1,20.9890407160248,40.2334685909601,560096607,1292502,66
FP2,20.7915502371247,40.1392343125345,560164915,1292502,66"""
            input_df = pd.read_csv(StringIO(csv_string))

    elif csv_url_or_path is not None:
        LOGGER.debug(f'Reading CSV: {csv_url_or_path}')
        # Try with a comma separator first:
        input_df = pd.read_csv(csv_url_or_path)
        # If that failed, try semicolon:
        if input_df.shape[1] == 1:
            LOGGER.debug(f'Found only one column (name "{input_df.columns}"). Maybe it is not comma-separated, but comma-separated? Trying...')
            input_df = pd.read_csv(csv_url_or_path, sep=';')

    # Open a database connection
    LOGGER.debug(f'Connect to database...')
    with open(config_file_path, 'r') as config_file:
        db_config = json.load(config_file)

    conn = get_connection_object_config(db_config)
    cursor = conn.cursor()

    # From the dataframe rows, make SQL rows to be inserted into a temporary table:
    list_of_insert_rows = make_insertion_rows_from_dataframe(input_df, colname_lon, colname_lat, colname_site_id)

    # Create the temp table and populate it
    # This has to be done differently, with everything as geography already!
    # So I defined a local copy of this function here:
    tablename, reg_ids = local_create_and_populate_temp_table(cursor, list_of_insert_rows)

    ##########################
    ### Nearest Neighbours ###
    ##########################

    # First, add the nearest neighbours to each point in the temp table:
    #_add_nearest_neighours_to_temptable(cursor, tablename, min_strahler)
    query = f'''
    ALTER TABLE {tablename}
        ADD COLUMN geog_closest geography(LINESTRING, 4326),
        ADD COLUMN subcid_closest integer,
        ADD COLUMN strahler_closest integer;
    '''
    cursor.execute(query)

    # Fill the template query with table name and min_strahler:
    query_nearest_neighbours = query_nearest_neighbours.format(
        min_strahler=min_strahler,
        tablename=tablename
    )

    LOGGER.info('Starting query: Nearest Neigbours')
    querystart = time.time()
    cursor.execute(query_nearest_neighbours)
    queryend = time.time()
    LOGGER.info('Finished query: Nearest Neigbours')
    LOGGER.debug(f'**** TIME ************: {(queryend - querystart)}')



    ###############################
    ### Snapping with distances ###
    ###############################

    if add_distance:

        # Compute snapped point, store in table:
        query = f'ALTER TABLE {tablename} ADD COLUMN geog_snapped geography(POINT, 4326)'
        cursor.execute(query)
        query = f'''
        UPDATE {tablename} AS temp
            SET geog_snapped = ST_LineInterpolatePoint(
                temp.geog_closest,
                ST_LineLocatePoint(temp.geog_closest, temp.geog_user)
            );
        '''
        LOGGER.info('Starting query: Snapping')
        querystart = time.time()
        cursor.execute(query)
        # Runs into:
        #psycopg2.errors.UndefinedFunction: function st_linelocatepoint(geography, geometry) does not exist
        #LINE 5:                 ST_LineLocatePoint(temp.geog_closest, temp.g...
        #                        ^
        #HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
        queryend = time.time()
        LOGGER.info('Finished query: Snapping')
        LOGGER.debug(f'**** TIME ************: {(queryend - querystart)}')

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
            ST_AsText(temp.geog_snapped),
            temp.strahler_closest,
            temp.subcid_closest,
            ST_Distance(
                temp.geog_user,
                temp.geog_snapped
            )
        FROM {tablename} AS temp;
        '''
        LOGGER.info('Starting query: Distance')
        querystart = time.time()
        cursor.execute(query)
        queryend = time.time()
        LOGGER.info('Finished query: Distance')
        LOGGER.debug(f'**** TIME ************: {(queryend - querystart)}')


    ##################################
    ### Snapping without distances ###
    ##################################

    if not add_distance:

        # This RETURNS the snapped points, but does not STORE them in the temp table!
        # Note: We have to cast the geography to geometry, otherwise we run into this error:
        #psycopg2.errors.UndefinedFunction: function st_linelocatepoint(geography, geometry) does not exist
        #LINE 9: ST_LineLocatePoint(temp.geom_closest, ST...
        #HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
        # Ah! Instead, we should probably have casted the second point to geography, instead of the first to geometry...
        query = f'''
        SELECT
            temp.lon,
            temp.lat,
            temp.site_id,
            ST_AsText(
                ST_LineInterpolatePoint(
                    temp.geog_closest,
                    ST_LineLocatePoint(temp.geog_closest, temp.geog_user, true)
                )
            ),
            temp.strahler_closest,
            temp.subcid_closest
        FROM {tablename} AS temp
        '''
        LOGGER.info('Starting query: Snapping without distance')
        querystart = time.time()
        cursor.execute(query)
        queryend = time.time()
        LOGGER.info('Finished query: Snapping without distance')
        LOGGER.debug(f'**** TIME ************: {(queryend - querystart)}')


    #############################################
    ### Make result dataframe from SQL result ###
    #############################################

    LOGGER.debug('Making dataframe from database result...')
    output_df = _package_result_in_dataframe(cursor, colname_lon, colname_lat, colname_site_id)
    LOGGER.debug(f'Storing dataframe to csv: {output_csv_name}...')
    output_df.to_csv(output_csv_name, index=False)

    # Database hygiene: Drop the table
    drop_temp_table(cursor, tablename)

    LOGGER.info(f'Tested query: {queryname}, started  at {now:%Y%m%d_%H-%M-%S}')
    now = datetime.now()
    LOGGER.info(f'Tested query: {queryname}, finished at {now:%Y%m%d_%H-%M-%S}')
    LOGGER.info('Done.')


### Own copy

def local_create_and_populate_temp_table(cursor, list_of_insert_rows, add_subcids=True):
    '''
    Creating a temp table containing the columns:
    site_id, lon, lat, subc_id, basin_id, reg_id, geog_user
    '''
    tablename =_tablename('pygeo')
    LOGGER.debug(f'Creating and populating temp table "{tablename}"...')

    # Create a temporary table with the basic information about the points:
    _create_temp_table(cursor, tablename)

    # Insert the information passed by the user:
    _fill_temp_table(cursor, tablename, list_of_insert_rows)

    # Generate a spatial index:
    _add_index(cursor, tablename)

    # For each point, find out and store and retrieve the reg_id:
    reg_id_set = _update_temp_table_regid(cursor, tablename)

    # For each point, find out and store the basin_id and subc_id:
    if add_subcids:
        _add_subcids(cursor, tablename, reg_id_set)
        LOGGER.debug(f'Populating temp table "{tablename}" (incl. subc_id, basin_id, reg_id)... done.')
    else:
        LOGGER.debug(f'Populating temp table "{tablename}" (incl. only reg_id)... done.')

    return tablename, reg_id_set


def _tablename(tablename_prefix):
    randomstring = str(uuid.uuid4()).replace('-', '')
    return f'{tablename_prefix}_{randomstring}'

def _create_temp_table(cursor, tablename):
    # Only difference to original:
    # ORIGINAL:         geom_user geometry(POINT, 4326)
    # HERE:             geog_user geography(POINT, 4326)

    LOGGER.debug(f'Creating temporary table "{tablename}"...')

    # TODO WIP numeric or decimal or ...?
    # TODO: Is varchar a good type for expected site_ids?
    query = f'''
    CREATE TEMP TABLE {tablename} (
        site_id varchar(100),
        lon decimal,
        lat decimal,
        subc_id integer,
        basin_id integer,
        reg_id smallint,
        geog_user geography(POINT, 4326)
    );
    '''

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'creating temp table')
    LOGGER.debug(f'Creating temporary table "{tablename}"... done.')
    return tablename

def _fill_temp_table(cursor, tablename, list_of_insert_rows):
    # Only difference to original:
    # ORIGINAL:         INSERT INTO ... lon, lat, geom_user) VALUES ...
    # HERE:             INSERT INTO ... lon, lat, geog_user) VALUES ...
    LOGGER.debug(f'Inserting into temporary table "{tablename}"...')
    LOGGER.log(logging.TRACE, f'INSERTION ROWS: {list_of_insert_rows}')

    list_of_insertions = ", ".join(list_of_insert_rows)
    query = f'INSERT INTO {tablename} (site_id, lon, lat, geog_user) VALUES {list_of_insertions};'

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'inserting into temp table')
    LOGGER.debug(f'Inserting into temporary table "{tablename}"... done.')

def _add_index(cursor, tablename):
    # Only difference to original:
    # ORIGINAL:         USING gist (geom_user);'
    # HERE:             USING gist (geog_user);'
    LOGGER.debug(f'Creating index for temporary table "{tablename}"...')

    query = f'CREATE INDEX IF NOT EXISTS temp_test_geom_user_idx ON {tablename} USING gist (geog_user);'

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'adding spatial index')

    LOGGER.debug(f'Creating index for temporary table "{tablename}"... done.')


def _update_temp_table_regid(cursor, tablename):
    # Only difference to original:
    # WHERE st_intersects({tablename}.geom_user, reg.geom)
    # WHERE st_intersects({tablename}.geog_user, reg.geom) # Uff this will fail!
    # WHERE st_intersects({tablename}.geog_user::geometry, reg.geom) # This might work?

    ## Add reg_id to temp table, get it returned:
    LOGGER.debug(f'Update reg_id (st_intersects) in temporary table "{tablename}"...')
    query = f'''
    WITH updater AS (
        UPDATE {tablename}
        SET reg_id = reg.reg_id
        FROM regional_units reg
        WHERE st_intersects({tablename}.geog_user, reg.geom)
        RETURNING {tablename}.reg_id
    )
    SELECT DISTINCT reg_id FROM updater;
    '''

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'updating temp table with reg_id')

    LOGGER.debug(f'Update reg_id (st_intersects) in temporary table "{tablename}"... done')

    ## Retrieve reg_id, for next query:
    LOGGER.log(logging.TRACE, 'Retrieving reg_ids (RETURNING from UPDATE query)...')
    reg_id_set = set()
    while (True):
        row = cursor.fetchone()
        if row is None: break
        reg_id = row[0]
        LOGGER.log(logging.TRACE, f'  Retrieved: {reg_id}')
        reg_id_set.add(reg_id)
    LOGGER.debug(f'Set of distinct reg_ids present in the temp table: {reg_id_set}')
    return reg_id_set

def _add_subcids(cursor, tablename, reg_ids):
    # ORIGINAL:         st_intersects({tablename}.geom_user, sub.geom)
    # HERE:             st_intersects({tablename}.geog_user, sub.geom)
    LOGGER.debug(f'Update subc_id, basin_id (st_intersects) in temporary table "{tablename}"...')
    reg_ids_string = ", ".join([str(elem) for elem in reg_ids])
    query = f'''
    UPDATE {tablename}
    SET
        subc_id = sub.subc_id,
        basin_id = sub.basin_id
    FROM sub_catchments sub
    WHERE
        st_intersects({tablename}.geog_user, sub.geom)
        AND sub.reg_id IN ({reg_ids_string});
    '''

    ### Query database:
    LOGGER.log(logging.TRACE, "SQL query: {query}")
    querystart = time.time()
    cursor.execute(query)
    log_query_time(querystart, 'updating temp table with subc_id and basin_id')
    LOGGER.debug(f'Update subc_id, basin_id (st_intersects) in temporary table "{tablename}"... done.')




# Finally run...
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.error('Failed! %s' % e) # to be sure to get the time of failure
        print('Failed. Stopping.')
        #sys.exit(1)
        raise e # to be sure to get the traceback.

