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


def get_dijkstra_ids_one_to_one(conn, start_subc_id, end_subc_id, reg_id, basin_id, silent=False):
    # INPUT:  subc_ids (start and end)
    # OUTPUT: subc_ids (the entire path, incl. start and end, as a list)

    if not silent:
        LOGGER.debug('Compute route between subc_id {start_subc_id} and {end_subc_id} (in basin {basin_id}, region {reg_id})')

    ## Construct SQL query:
    ## Inner SELECT: Returns what the pgr_dijkstra needs: (id, source, target, cost).
    ## We run pgr_routing as one-to-one here.
    ## pgr_routing returns: (seq, path_seq, node, edge, cost, agg_cost)
    ## We are only interested in the edges (subc_ids of the stream segments) along the path, so we only select
    ## the edge, which is the id of the edge (in the inner SELECT, we defined "subc_id" as the id).
    query = f'''
    SELECT
        edge
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
    '''.replace("    ", "").replace("\n", " ")

    ## Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ## Get results and make list:
    LOGGER.log(logging.TRACE, 'Iterating over the result rows...')
    # Adding start segment, as it is not included in database return:
    all_ids = [start_subc_id]
    while (True):
        row = cursor.fetchone()
        if row is None: break
        subc_id = row[0]
        if subc_id == -1: # pgr_dijkstra returns -1 as the last edge...
            pass
        else:
            # add integer subc_id to list:
            all_ids.append(subc_id)

    return all_ids


def get_dijkstra_ids_to_outlet_plural(conn, input_df, colname_site_id, return_csv=False, return_json=False):
    # TODO: Add input GeoJSON!
    # We don't want a matrix, we want one path per pair of points - but for many!
    # INPUT:  CSV
    # OUTPUT: JSON or CSV (but ugly CSV... as we have to store entire paths in one column.)
    if not (return_csv or return_json): return_csv = True

    departing_points = _collect_departing_points_by_region_and_basin(input_df, colname_site_id)
    if return_csv:
        return _iterate_outlets_dataframe(conn, departing_points)
    elif return_json:
        return _iterate_outlets_json(conn, departing_points)


def _collect_departing_points_by_region_and_basin(input_df, colname_site_id):

    # First, collect all departing points by iterating over an input dataframe.
    # Store them by region_id and by basin_id, as we need those two values for
    # the query.
    departing_points = {}

    # Retrieve using column index, not colname - this is faster:
    colidx_site_id  = input_df.columns.get_loc(colname_site_id)
    colidx_subc_id  = input_df.columns.get_loc("subc_id")
    colidx_basin_id = input_df.columns.get_loc("basin_id")
    colidx_reg_id   = input_df.columns.get_loc("reg_id")

    i = 0
    for row in input_df.itertuples(index=False):
        i += 1

        # Extract values from CSV:
        site_id  = row[colidx_site_id]  # string
        subc_id  = row[colidx_subc_id]  # nullable int
        basin_id = row[colidx_basin_id] # nullable int
        reg_id   = row[colidx_reg_id]   # nullable int

        # Stop if no site_id!
        # Does this work with strings?
        if pd.isna(site_id):
            err_msg = f"Missing site_id in row {i} (subc_id={subc_id}, basin_id={basin_id}, reg_id={reg_id})"
            LOGGER.error(f'({i}) {err_msg}')
            raise ValueError(err_msg)
            continue

        # No subc_id, e.g. ocean case (point falls into the ocean):
        # As we do not return the site_ids, but only subc_ids with downstream path, there is no point in returning
        # an empty list for subc_id "NaN"... If we ever switch back to returning downstream ids for each site_id,
        # returning an empty list here make sense...
        if pd.isna(subc_id):
            msg = f"Cannot compute downstream ids due to missing subc_id at site '{site_id}'."
            LOGGER.info(msg)
            reg_id = basin_id = subc_id = None
            # We catch this "None" later and add it to the result, so that at least the user is informed.

        # Unexpected case...
        elif pd.isna(basin_id) or pd.isna(reg_id):
            LOGGER.error('TODO CAN THIS HAPPEN AT ALL? Can we have invalid reg_id/basin_id if we have no valid subc_id?')
            err_msg = f"UNEXPECTED: Cannot compute downstream ids due to missing value(s) at site {site_id} (subc_id={subc_id}, basin_id={basin_id}, reg_id={reg_id})"
            LOGGER.error(f'({i}) {err_msg}')
            raise ValueError(err_msg)

        # Store departing point in dictionary
        # using integers as keys
        LOGGER.log(logging.TRACE, f'({i}) Storing departure point for site {site_id} / for subc_id {subc_id}')
        if not reg_id in departing_points.keys():
            departing_points[reg_id] = {}
        if not basin_id in departing_points[reg_id].keys():
            departing_points[reg_id][basin_id] = {}
        if not subc_id in departing_points[reg_id][basin_id].keys():
            departing_points[reg_id][basin_id][subc_id] = set()
        departing_points[reg_id][basin_id][subc_id].add(site_id)

    LOGGER.info(f'Departing points (sorted by reg_id, basin_id, subc_id): {departing_points}')
    return departing_points


def _iterate_outlets_dataframe(conn, departing_points):

    # Will construct a dataframe from this:
    everything = []

    # Iterate over all regions/basins:
    # Note: All ids are strings, so we cast to int, and the site_ids are a set of strings
    for reg_id, all_basins in departing_points.items():

        # Add empty item for ocean case:
        if reg_id is None:
            all_site_ids = all_basins[None][None]
            LOGGER.debug(f'Compute paths to outlet for these sites not possible: {all_site_ids}')
            site_ids_str    = '+'.join(map(str, all_site_ids))
            everything.append([None, None, None, None, site_ids_str])
            continue

        # Now, for each basin, run a one-to-many routing query,
        # as one basin has just one outlet:
        reg_id = int(reg_id)
        for basin_id, all_subcids in all_basins.items():
            LOGGER.debug(f'Basin: {basin_id} (in regional unit {reg_id})')
            basin_id = int(basin_id)
            outlet_id = -basin_id
            start_ids = all_subcids.keys() # strings
            segments_by_start_id = get_dijkstra_ids_one_to_many(conn, start_ids, outlet_id, reg_id, basin_id)
            # This returned a dict: One list of segment ids (the path to outlet) per start subc_id.

            # Package in JSON list:
            for start_id, segment_ids in segments_by_start_id.items():
                all_site_ids = all_subcids[start_id]

                # Collect results per subcid / per departure point:
                # Output CSV:  We need to make one string out of the segment ids!
                # TODO: Separating the segment ids by "+" is not cool, but how to do it...
                segment_ids_str = '+'.join(map(str, segment_ids))
                site_ids_str    = '+'.join(map(str, all_site_ids))
                everything.append([reg_id, basin_id, start_id, segment_ids_str, site_ids_str])

    # Finished collecting the results, now return dataframe:
    output_df = pd.DataFrame(everything,
        columns=['reg_id', 'basin_id', 'subc_id', 'downstream_segments', 'site_ids']
    ).astype({
        'reg_id':   'Int64', # nullable int
        'basin_id': 'Int64', # nullable int
        'subc_id':  'Int64', # nullable int
        'downstream_segments': 'string',
        'site_ids': 'string'
    })
    return output_df


def _iterate_outlets_json(conn, departing_points):

    # Either return as a JSON dictionary, by subc_id:
    everything = {}
    # Or return as a JSON list:
    #everything = []

    # Iterate over all regions/basins:
    # Note: All ids are strings, so we cast to int, and the site_ids are a set of strings
    for reg_id, all_basins in departing_points.items():

        # Add empty item for ocean case:
        if reg_id is None:
            all_site_ids = all_basins[None][None]
            LOGGER.debug(f'Compute paths to outlet for these sites not possible: {all_site_ids}')
            basin_id = None
            #everything.append({...})
            everything[None] = {
                "subc_id": None,
                "basin_id": None,
                "outlet_id": None,
                "reg_id": None,
                "num_downstream_ids": None,
                "downstream_segments": None,
                "site_ids": list(all_site_ids)
            }
            continue

        # Now, for each basin, run a one-to-many routing query,
        # as one basin has just one outlet:
        reg_id = int(reg_id)
        for basin_id, all_subcids in all_basins.items():
            LOGGER.debug(f'Basin: {basin_id} (in regional unit {reg_id})')
            basin_id = int(basin_id)
            outlet_id = -basin_id
            start_ids = all_subcids.keys()
            segments_by_start_id = get_dijkstra_ids_one_to_many(conn, start_ids, outlet_id, reg_id, basin_id)
            # This returned a dict: One list of segment ids (the path to outlet) per start subc_id.

            # Package in JSON list:
            for start_id, segment_ids in segments_by_start_id.items():
                all_site_ids = all_subcids[start_id]
                # Either return as a JSON list:
                #everything.append({...})
                # Or return as a JSON dictionary, by subc_id:
                everything[int(start_id)] = {
                    "subc_id": int(start_id),
                    "basin_id": basin_id,
                    "outlet_id": outlet_id,
                    "reg_id": reg_id,
                    "num_downstream_ids": len(segment_ids),
                    "downstream_segments": segment_ids,
                    "site_ids": list(all_site_ids)
                }

    # Finished collecting the results, now return JSON object:
    return everything


def get_dijkstra_ids_many_to_many(conn, subc_ids_start, subc_ids_end, reg_id, basin_id, result_format='json'):
    # INPUT:  Sets of subc_ids
    # OUTPUT: Route matrix (as JSON)

    LOGGER.debug(f'Compute path matrix between {len(subc_ids_start | subc_ids_end)} subc_ids (in basin {basin_id}, region {reg_id})')
    # TODO What if not in one basin?

    ## Construct SQL query:
    ## Inner SELECT: Returns what the pgr_dijkstra needs: (id, source, target, cost).
    ## We run pgr_routing as many-to-many here: 
    ##   We compute the paths from each start point to each end point,
    ##   resulting in a matrix of paths (path from s1 to e1, from s1 to e2, from s1 to e3, ...).
    ##   Each path consists of many edges/stream segments!
    ## pgr_routing returns: (seq, path_seq, start_vid, end_vid, node, edge, cost, agg_cost)
    ##   where start_vid and end_vid tell us which path, and then node/edge are the stream segment.
    ## We are interested in all the edges (subc_ids of the stream segments) along the path,
    ##   and for identifying the path, we need the ids of the start and end, so we select
    ##   start_vid, end_vid and edge.
    nodes_start = ','.join(map(str, subc_ids_start))
    nodes_end   = ','.join(map(str, subc_ids_end))
    query = f'''
    SELECT
        start_vid,
        end_vid,
        edge
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
    # SQL query: SELECT edge FROM pgr_dijkstra(' SELECT   subc_id AS id,   subc_id AS source,   target,   length AS cost   FROM hydro.stream_segments   WHERE reg_id = 58   AND basin_id = 1294020', ARRAY[507294699,507282720,507199553,507332148,507290955], ARRAY[507294699,507282720,507199553,507332148,507290955], directed := false);
    
    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

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
    # numpy.core._exceptions._UFuncNoLoopError: ufunc 'greater' did not contain a loop with signature matching types
    # Note: Values have to be native python integers, not numpy integers, otherwise
    # pygeoapi will cause an error later on when serializing the results:
    # Object of type int64 is not JSON serializable
    result_matrix = {}
    for start_id in subc_ids_start:
        result_matrix[str(start_id)] = {}
        for end_id in subc_ids_end:
            result_matrix[str(start_id)][str(end_id)] = [int(start_id)] # TODO: check: Have to add start id?

    ## Iterating over the result rows:
    LOGGER.log(logging.TRACE, f"Result matrix to be filled: {result_matrix}")
    LOGGER.log(logging.TRACE, "Iterating over results...")
    while True:
        row = cursor.fetchone()
        if row is None: break
        
        # Collect all the ids along the paths:
        # Each path is defined by start and end, and consists of many edges/stream segments.
        start_id  = str(row[0]) # start
        end_id    = str(row[1]) # end
        this_id   = int(row[2]) # current edge/stream segment as integer
        if this_id == -1:
            pass
        else:
            # Add this subc_id (integer) to the matrix
            # (to the list of stream segments for this start-end-combination).
            result_matrix[start_id][end_id].append(this_id)
            LOGGER.log(logging.TRACE, 'Start {start_id} to end {end_id}, add this id {this_id}')

    LOGGER.log(logging.TRACE, "Iterating over results... DONE.")
    #LOGGER.log(logging.TRACE, f"JSON result: {result_matrix}") # quite big!

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

        # Get the row of the matrix with the paths from start id to all end ids:
        start_id = str(start_id)
        matrix_row = result_matrix[start_id]
        LOGGER.debug(f'Matrix row for {start_id}: {matrix_row}')

        # First item in the dataframe row is the start id:
        row_dataframe = [start_id]

        # Now fill with the distances:
        for end_id in colnames:

            # First colname is not an end id, but the title for the first column...
            if end_id == 'subc_ids':
                continue

            # Retrieve the distance and add to row:
            path_list = matrix_row[str(end_id)]
            path_list_str = '+'.join(map(str, path_list))
            row_dataframe.append(path_list_str)

        all_rows.append(row_dataframe)

    output_df = pd.DataFrame(all_rows, columns=colnames)
    return output_df


def get_dijkstra_ids_one_to_many(conn, start_subc_ids, end_subc_id, reg_id, basin_id):
    # INPUT:  Set of subc_ids (in one basin)
    # OUTPUT: JSON dict: One path (list of subc_ids) per start_subc_id.

    LOGGER.debug(f'Compute paths from {len(start_subc_ids)} subc_ids to outlet (in basin {basin_id}, region {reg_id})')

    ## Construct SQL query:
    ## Inner SELECT: Returns what the pgr_dijkstra needs: (id, source, target, cost).
    ## We run pgr_routing as one-to-many here:
    ##   We compute the paths from each start point to one end point (the outlet),
    ##   resulting in a list of paths (path from s1 to o, from s1 to o, from s1 to o, ...).
    ##   Each path consists of many edges/stream segments!
    ## pgr_routing returns: (seq, path_seq, start_vid, node, edge, cost, agg_cost)
    ##   where start_vid tells us which path, and then node/edge are the stream segments.
    ## We are interested in all the edges (subc_ids of the stream segments) along the path,
    ##   and for identifying the path, we need the id of the start, so we select
    ##   start_vid and edge.
    start_nodes = ','.join(map(str, start_subc_ids))
    query = f'''
    SELECT
        start_vid,
        edge
    FROM pgr_dijkstra(
        'SELECT
            subc_id AS id,
            subc_id AS source,
            target,
            length AS cost
                FROM hydro.stream_segments
                WHERE reg_id = {reg_id}
                AND basin_id = {basin_id}',
        ARRAY[{start_nodes}],
        {end_subc_id},
        directed := false
    );
    '''.replace("\n", " ").replace("    ", "").strip()
    LOGGER.log(logging.TRACE, f"SQL query: {query}")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ## Construct result JSON dict (using integer key):
    segments_by_start_id = {}
    for start_id in start_subc_ids:
        segments_by_start_id[start_id] = []

    ## Iterating over the result rows:
    LOGGER.log(logging.TRACE, f"Result dict to be filled: {segments_by_start_id}")
    LOGGER.log(logging.TRACE, "Iterating over results...")
    while True:
        row = cursor.fetchone()
        if row is None: break

        # Collect all the ids along the paths:
        # Each path is defined by its start, and consists of many edges/stream segments.
        start_id  = row[0] # departure point subc_id(integer)
        this_id   = row[1] # current edge/stream segment (integer)
        if this_id == -1:
            pass
        else:
            # Add this subc_id (integer) to the matrix
            # (to the list of stream segments for this start-end-combination).
            segments_by_start_id[start_id].append(this_id)
            LOGGER.log(logging.TRACE, 'Start {start_id} to end {end_id}, add this id {this_id}')

    LOGGER.log(logging.TRACE, "Iterating over results... DONE.")

    return segments_by_start_id

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
    LOGGER.debug('Connecting to database... DONE.')


#################
### One route ###
#################

if __name__ == "__main__" and True:

    ## One example, returns 200 subc_ids:
    subc_id_start  = 507294699
    subc_id_end = 507282720
    basin_id = 1294020
    reg_id = 58
    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_one_to_one (will return 200 ids)')
    res = get_dijkstra_ids_one_to_one(conn, subc_id_start, subc_id_end, reg_id, basin_id)
    print(f'RESULT: ROUTE:\n{res}') # just the list of 200 ids

    ## Another example, returns 5 subc_ids:
    #lon_start = 9.937520027160646
    #lat_start = 54.69422745526058
    subc_id_start = 506251713
    #lon_end = 9.9217
    #lat_end = 54.6917
    subc_id_end = 506251712
    basin_id = 1292547
    reg_id = 58
    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_one_to_one (will return 5 ids)')
    res = get_dijkstra_ids_one_to_one(conn, subc_id_start, subc_id_end, reg_id, basin_id)
    print(f'RESULT: ROUTE:\n{res}') # just the list of ids


    ######################
    ### Add geometries ###
    ######################

    print('\nSTART PACKAGING IN GEOJSON...')

    # If you then want geometries for this, use functions
    # "get_streamsegment_linestrings_feature_coll" and
    # "get_streamsegment_linestrings_geometry_coll"
    # from module aqua90m.geofresh.get_linestrings:
    import get_linestrings

    # GeometryColl
    geom_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, res, basin_id, reg_id)
    print(f'\nRESULT (GeometryCollection):\n{geom_coll}')

    # Feature Coll
    feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(conn, res, basin_id, reg_id)
    print(f'\nRESULT (FeatureCollection/LineStrings):\n{feature_coll}')


###################
### Many routes ###
###################

if __name__ == "__main__" and True:

    other1 = 507199553
    other2 = 507332148
    other3 = 507290955

    ## With few points:
    start_ids = [subc_id_start, subc_id_end, other1]
    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_many_to_many')
    res = get_dijkstra_ids_many_to_many(conn, start_ids, reg_id, basin_id)
    print(f'RESULT: ROUTE MATRIX: {res}')

    ## With more points:
    start_ids = [subc_id_start, subc_id_end, other1, other2, other3]
    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_many_to_many')
    res = get_dijkstra_ids_many_to_many(conn, start_ids, reg_id, basin_id)
    print(f'RESULT: ROUTE MATRIX: {res}')


##########################
### To outlet, looping ###
##########################

if __name__ == "__main__" and True:
    import basic_queries

    # Input: dataframe, output dataframe, with site_id!
    # Note: g, gg, ggg are in the same subcatchment.

    # More points, two basins/regional units:
    input_df = pd.DataFrame(
        [
            ['a',  10.698832912677716, 53.51710727672125],
            ['b',  12.80898022975407,  52.42187129944509],
            ['c',  11.915323076217902, 52.730867141970464],
            ['d',  16.651903948708565, 48.27779486850176],
            ['e',  19.201146608148463, 47.12192880511424],
            ['f',  24.432498016999062, 61.215505889934434],
            ['sea',  8.090485, 54.119322],
            ['g', 10.041155219078064, 53.07006147583069],
            ['gg', 10.042726993560791, 53.06911450500803],
            ['ggg', 10.039894580841064, 53.06869677412868]
        ], columns=['site_id', 'lon', 'lat']
    )

    # Less points, just one basin:
    #input_df = pd.DataFrame(
    #    [
    #        ['a',  10.698832912677716, 53.51710727672125],
    #        ['b',  12.80898022975407,  52.42187129944509],
    #        ['g', 10.041155219078064, 53.07006147583069],
    #        ['gg', 10.042726993560791, 53.06911450500803],
    #        ['ggg', 10.039894580841064, 53.06869677412868]
    #    ], columns=['site_id', 'lon', 'lat']
    #)

    print('\nPREPARE RUNNING FUNCTION: get_dijkstra_ids_to_outlet_plural')
    ## Now, for each row, get the ids!
    temp_df = basic_queries.get_subcid_basinid_regid_for_all_1csv(conn, LOGGER, input_df, "lon", "lat", "site_id")
    print(f'\n{temp_df}')
    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_to_outlet_plural')
    res = get_dijkstra_ids_to_outlet_plural(conn, temp_df, "site_id", return_csv=True)
    print(f'RESULT: SEGMENTS IN DATAFRAME: {res}')

    print('\nPREPARE RUNNING FUNCTION: get_dijkstra_ids_to_outlet_plural')
    ## Now, for each row, get the ids!
    temp_df = basic_queries.get_subcid_basinid_regid_for_all_1csv(conn, LOGGER, input_df, "lon", "lat", "site_id")
    print(f'\n{temp_df}')
    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_to_outlet_plural')
    res = get_dijkstra_ids_to_outlet_plural(conn, temp_df, "site_id", return_json=True)
    print(f'RESULT: SEGMENTS IN JSON: {res}')


###################
### Finally ... ###
###################

if __name__ == "__main__":
    conn.close()
