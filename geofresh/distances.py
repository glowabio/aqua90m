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
    '''.replace("\n", " ").replace("    ", "").strip()
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


def get_dijkstra_distance_many(conn, subc_ids_start, subc_ids_end, reg_id, basin_id):
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
    '''


    ## Construct SQL query:
    nodes_start = 'ARRAY[%s]' % ','.join(str(x) for x in subc_ids_start)
    nodes_end   = 'ARRAY[%s]' % ','.join(str(x) for x in subc_ids_end)
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
        {nodes_start},
        {nodes_end},
        directed := false
    );
    '''.replace("\n", " ").replace("    ", "").strip()
    LOGGER.log(logging.TRACE, f"SQL query: {query}")
    
    ## Query database:
    cursor = conn.cursor()
    cursor.execute(query)

    ## Extract results, first as a matrix (nested dict):
    json_matrix = _result_to_matrix(cursor, subc_ids_start, subc_ids_end)
    return json_matrix


def _result_to_matrix(cursor, subc_ids_start, subc_ids_end):

    ## Construct result matrix:
    # TODO: JSON may not be the ideal type for returning a matrix!
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
        basin_id)
    print(f'\nRESULT: DISTANCE MATRIX\n{matrix}')

    ## With more points:
    start_ids = set([subc_id_start, subc_id_end, other1, other2, other3])
    end_ids   = set([subc_id_start, subc_id_end, other1, other2, other3])
    print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_many')
    matrix = get_dijkstra_distance_many(conn,
        start_ids,
        end_ids,
        reg_id,
        basin_id)
    print(f'\nRESULT: DISTANCE MATRIX:\n{matrix}')



###################
### Finally ... ###
###################

conn.close()
