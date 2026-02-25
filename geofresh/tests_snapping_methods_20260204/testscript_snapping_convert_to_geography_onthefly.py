import logging
import time
import json
import pandas as pd
from io import StringIO
from datetime import datetime


from temp_table_for_queries import make_insertion_rows_from_dataframe
from temp_table_for_queries import create_and_populate_temp_table
from temp_table_for_queries import drop_temp_table
from snapping_strahler import _package_result_in_dataframe
from database_connection import get_connection_object_config

#LOGGER = logging.getLogger(__name__)
LOGGER = logging.getLogger('testscript')


### Nearest neighbour query
query_nearest_with_geography = '''
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
        ORDER BY seg.geom::geography <-> temp2.geom_user::geography
        LIMIT 1
    ) AS closest
    WHERE temp1.geom_user = temp2.geom_user;
'''


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
    query_nearest_neighbours = query_nearest_with_geography
    queryname = 'query_nearest_with_geography'
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
    tablename, reg_ids = create_and_populate_temp_table(cursor, list_of_insert_rows)

    ##########################
    ### Nearest Neighbours ###
    ##########################

    # First, add the nearest neighbours to each point in the temp table:
    #_add_nearest_neighours_to_temptable(cursor, tablename, min_strahler)
    query = f'''
    ALTER TABLE {tablename}
        ADD COLUMN geom_closest geography(LINESTRING, 4326),
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
        query = f'ALTER TABLE {tablename} ADD COLUMN geom_snapped geometry(POINT, 4326)'
        cursor.execute(query)
        query = f'''
        UPDATE {tablename} AS temp
            SET geom_snapped = ST_LineInterpolatePoint(
                temp.geom_closest,
                ST_LineLocatePoint(temp.geom_closest, temp.geom_user)
            );
        '''
        LOGGER.info('Starting query: Snapping')
        querystart = time.time()
        cursor.execute(query)
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
            ST_AsText(temp.geom_snapped),
            temp.strahler_closest,
            temp.subcid_closest,
            ST_Distance(
                temp.geom_user::geography,
                temp.geom_snapped::geography
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
        query = f'''
        SELECT
            temp.lon,
            temp.lat,
            temp.site_id,
            ST_AsText(
                ST_LineInterpolatePoint(
                    temp.geom_closest::geometry,
                    ST_LineLocatePoint(temp.geom_closest::geometry, temp.geom_user)
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


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.error('Failed! %s' % e) # to be sure to get the time of failure
        print('Failed. Stopping.')
        #sys.exit(1)
        raise e # to be sure to get the traceback.

