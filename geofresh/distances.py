import json
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import pandas as pd


'''

These functions all call the pgr_dijkstra algorithm on PostGIS,
but they may run on different inputs (e.g. one point to one point
resulting into one path, or many point to many points resulting
in a matrix of paths), or are interested in different outcomes
(e.g. the length of the segments, or the segment ids).

Many of these could be run with the same query on the database,
but to optimize for efficiency, each time, we will only request
those fields that we actually need.

Table stream_segments contains:
# subc_id, target, length, cum_length, flow_accum, basin_id, strahler, reg_id, geom, geom_fix

pgr_dijktra() returns:
https://docs.pgrouting.org/3.4/en/pgr_dijkstra.html#many-to-many
many-to-many:
RETURNS SET OF (seq, path_seq, start_vid, end_vid, node, edge, cost, agg_cost)
one-to-one
RETURNS SET OF (seq, path_seq, node, edge, cost, agg_cost)
'''

def get_dijkstra_distance_one(conn, start_subc_id, end_subc_id, reg_id, basin_id):
    # This simply returns one number!
    # INPUT:  Start and end (subc_id)
    # OUTPUT: The distance (one number), which is the accumulated "length" attribute.
    LOGGER.debug(f'Compute distance between subc_id {start_subc_id} and {end_subc_id} (in basin {basin_id}, region {reg_id})')

    '''
    The distance between two points (507291111, 507292222) is just a number.

    Expressed like a matrix below, it would look like this (totally overdone):
    {
      '507291111': {'507291111': 0,     '507292222': 771.3, },
      '507292222': {'507291111': 771.3, '507292222': 0}
    }
    '''

    ## Construct SQL query:
    ## We return the aggregated cost over the path, and as cost we use the attribute "length".
    query = f'''
    SELECT
        edge,
        agg_cost
    FROM pgr_dijkstra(
        'SELECT
            subc_id AS id,
            subc_id AS source,
            target,
            length AS cost
            FROM hydro.stream_segments
            WHERE reg_id = {reg_id}
            AND basin_id = {basin_id}',
        {start_subc_id},
        {end_subc_id},
        directed := false
    );
    '''
    LOGGER.log(logging.TRACE, f"SQL query: {query}")

    ## Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ## Iterate over the result rows to find the last row, and then get the accumulated
    ## cost (which is the "length"), returned by the algorithm:
    dist = None
    while True:
        row = cursor.fetchone()
        if row is None: break
        #edge = row[0]
        #agg_cost = row[1]
        if row[0] == -1: # pgr_dijkstra returns -1 as the last edge...
            dist = row[1]

    return dist


def get_dijkstra_distance_many(conn, subc_ids_start, subc_ids_end, reg_id, basin_id, result_format='json'):
    # INPUT: Sets of subc_ids
    # OUTPUT: Distance matrix (as JSON)

    LOGGER.debug(f'Compute distance matrix between {len(subc_ids_start | subc_ids_end)} subc_ids (in basin {basin_id}, region {reg_id})')
    # TODO What if not in one basin?

    '''
    Example output: It's a JSONified matrix!
    {
        "507282720": {"507282720": 0,                 "507199553": 64695.14314651489, "507290955": 123218.3441696167, "507294699": 77136.30451583862, "507332148": 78088.08876419067 },
        "507199553": {"507282720": 64695.14314651489, "507199553": 0,                 "507290955": 92310.46207046509, "507294699": 46228.42241668701, "507332148": 47180.20666503906 },
        "507290955": {"507282720": 123218.3441696167, "507199553": 92310.46207046509, "507290955": 0,                 "507294699": 74875.18420028687, "507332148": 75826.96844863892 },
        "507294699": {"507282720": 77136.30451583862, "507199553": 46228.42241668701, "507290955": 74875.18420028687, "507294699": 0,                 "507332148": 14313.996437072754},
        "507332148": {"507282720": 78088.08876419067, "507199553": 47180.20666503906, "507290955": 75826.96844863892, "507294699": 14313.996437072754,"507332148": 0                 }
    }

    As a dataframe:
        subc_ids      507282720     507199553      507290955     507294699     507332148
    0  507282720       0.000000  64695.143147  123218.344170  77136.304516  78088.088764
    1  507199553   64695.143147      0.000000   92310.462070  46228.422417  47180.206665
    2  507290955  123218.344170  92310.462070       0.000000  74875.184200  75826.968449
    3  507294699   77136.304516  46228.422417   74875.184200      0.000000  14313.996437
    4  507332148   78088.088764  47180.206665   75826.968449  14313.996437      0.000000
    '''


    ## Construct SQL query:
    nodes_start = ','.join(map(str, subc_ids_start))
    nodes_end   = ','.join(map(str, subc_ids_end))
    query = f'''
    SELECT 
        edge,
        start_vid,
        end_vid,
        agg_cost
    FROM pgr_dijkstra(
        'SELECT
            subc_id AS id,
            subc_id AS source,
            target,
            length AS cost
            FROM hydro.stream_segments
            WHERE reg_id = {reg_id}
            AND basin_id = {basin_id}',
        ARRAY[{nodes_start}],
        ARRAY[{nodes_end}],
        directed := false
    );
    '''
    LOGGER.log(logging.TRACE, f"SQL query: {query}")
    
    ## Query database:
    cursor = conn.cursor()
    cursor.execute(query)

    ## Extract results, first as a matrix (nested dict):
    json_matrix = _result_to_matrix(cursor, subc_ids_start, subc_ids_end)

    ## Make a dataframe from this, if requested:
    if result_format == 'json':
        return json_matrix
    elif result_format == 'dataframe':
        output_df = _matrix_to_dataframe(json_matrix, subc_ids_start, subc_ids_end)
        return output_df
    else:
        raise ValueError(f'Unknown result format: {result_format}. Expected json or dataframe.')


def _result_to_matrix(cursor, subc_ids_start, subc_ids_end):

    ## Construct result matrix:
    # TODO: JSON may not be the ideal type for returning a matrix!
    # Note: Keys have to strings, otherwise pygeoapi will cause an
    # error later on when serializing the results:
    # "numpy.core._exceptions._UFuncNoLoopError: ufunc 'greater' did not contain a loop with signature matching types"
    # Note: Values have to be native python numbers, not numpy numbers, otherwise
    # pygeoapi will cause an error later on when serializing the results:
    # "Object of type int64 is not JSON serializable"
    # For some reason, this does not seem to be a problem here, so we don't
    # have to cast, as we do in routing.py
    result_matrix = {}
    for start_id in subc_ids_start:
        result_matrix[str(start_id)] = {}
        for end_id in subc_ids_end:
            result_matrix[str(start_id)][str(end_id)] = 0

    ## Iterate over the result rows:
    while True:
        row = cursor.fetchone()
        if row is None: break
        
        # We only look at the last edge of a path, as PostGIS returns agg_cost for us!
        if row[0] == -1: # if edge is -1...
            # Retrieve both nodes of the edge, and the accumulated cost/length:
            start_id  = str(row[1])
            end_id    = str(row[2])
            agg_cost  = row[3]
            # Add this subc_id (integer) to the result matrix
            # (store the distance for this start-end-combination).
            result_matrix[start_id][end_id] = agg_cost
            LOGGER.log(logging.TRACE, f'Start {start_id} to end {end_id}, accumulated length {agg_cost}')

    return result_matrix

def _matrix_to_dataframe(result_matrix, subc_ids_start, subc_ids_end):

    # Construct result dataframe:
    #   Basically a matrix, as dataframe/table/csv:
    #   Column names will be the end subc_ids (first column contains the end subc_ids)
    #   Row names will be the start subc_ids (first row contains the start subc_ids)
    all_rows = []

    # Define column names for dataframe: end ids
    # First column contains the subcids
    colnames = ["subc_ids"]
    for end_id in subc_ids_end:
        colnames.append(str(end_id))

    # Fill all dataframe rows with distances
    for start_id in subc_ids_start:

        # Get the row of the matrix with the distances from start id to all end ids:
        start_id = str(start_id)
        matrix_row = result_matrix[start_id]
        #LOGGER.debug(f'Matrix row for {start_id}: {matrix_row}')

        # First item in the dataframe row is the start id:
        row_dataframe = [start_id]

        # Now fill with the distances:
        for end_id in colnames:

            # First colname is not an end id, but the title for the first column...
            if end_id == 'subc_ids':
                continue

            # Retrieve the distance and add to row:
            distance = matrix_row[str(end_id)]
            row_dataframe.append(distance)

        all_rows.append(row_dataframe)

    output_df = pd.DataFrame(all_rows, columns=colnames)
    return output_df



###############
### Testing ###
###############

if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
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



####################
### One distance ###
####################

if __name__ == "__main__" and True:

    subc_id_start  = 507294699
    subc_id_end = 507282720
    basin_id = 1294020
    reg_id = 58

    print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_one')
    res = get_dijkstra_distance_one(conn, subc_id_start, subc_id_end, reg_id, basin_id)
    print(f'RESULT: DISTANCE:\n{res}') # just a number!


######################
### Many distances ###
######################

if __name__ == "__main__" and True:

    other1 = 507199553
    other2 = 507332148
    other3 = 507290955


    ## With few points:
    start_ids = set([subc_id_start, subc_id_end, other1])
    end_ids   = set([subc_id_start, subc_id_end, other1])
    print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_many')
    matrix = get_dijkstra_distance_many(conn,
        start_ids,
        end_ids,
        reg_id,
        basin_id,
        'json')
    print(f'\nRESULT: DISTANCE MATRIX\n{matrix}')
    dataframe = _matrix_to_dataframe(matrix, start_ids, end_ids)
    print(f'\nRESULT: DISTANCE DATAFRAME:\n{dataframe}')

    ## With few points, asking directly for dataframe:
    start_ids = set([subc_id_start, subc_id_end, other1])
    end_ids   = set([subc_id_start, subc_id_end, other1])
    print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_many')
    dataframe = get_dijkstra_distance_many(conn,
        start_ids,
        end_ids,
        reg_id,
        basin_id,
        'dataframe')
    print(f'\nRESULT: DISTANCE DATAFRAME:\n{dataframe}')

    ## With more points:
    start_ids = set([subc_id_start, subc_id_end, other1, other2, other3])
    end_ids   = set([subc_id_start, subc_id_end, other1, other2, other3])
    print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_many')
    matrix = get_dijkstra_distance_many(conn,
        start_ids,
        end_ids,
        reg_id,
        basin_id,
        'json')
    print(f'\nRESULT: DISTANCE MATRIX:\n{matrix}')
    dataframe = _matrix_to_dataframe(matrix, start_ids, end_ids)
    print(f'\nRESULT: DISTANCE DATAFRAME:\n{dataframe}')


###################
### Finally ... ###
###################
if __name__ == "__main__":
    conn.close()
