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


def get_dijkstra_ids_one(conn, start_subc_id, end_subc_id, reg_id, basin_id, silent=False):
    #INPUT: subc_ids (start and end)
    #OUTPUT: subc_ids (the entire path, incl. start and end)
    if not silent:
        LOGGER.debug('Compute route between subc_id %s and %s (in basin %s, region %s)' % (start_subc_id, end_subc_id, basin_id, reg_id))


    ### Define query:
    ### Construct SQL query:
    query = 'SELECT edge' # We are only interested in the subc_id of each line segment along the path.
    query += '''
    FROM pgr_dijkstra('
        SELECT
        subc_id AS id,
        subc_id AS source,
        target,
        length AS cost
        FROM hydro.stream_segments
        WHERE reg_id = {reg_id}
        AND basin_id = {basin_id}',
        {start_subc_id},
        {end_subc_id},
        directed := false);
    '''.format(reg_id = reg_id, basin_id = basin_id, start_subc_id = start_subc_id, end_subc_id = end_subc_id)
    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.log(logging.TRACE, 'Iterating over the result rows...')
    all_ids = [start_subc_id] # Adding start segment, as it is not included in database return!
    while (True):
        row = cursor.fetchone()
        if row is None: break
        edge = row[0]

        if edge == -1: # pgr_dijkstra returns -1 as the last edge...
            pass

        else:
            all_ids.append(edge) # these are already integer!

    return all_ids


def get_dijkstra_ids_to_outlet_one_loop(conn, input_df, colname_site_id, return_csv=False, return_json=False):
    # We don't want a matrix, we want one path per pair of points - but for many!
    # Input: CSV
    # Output: JSON or CSV (but ugly CSV... as we have to store entire paths in one column.)
    everything = []
    already_computed = set()

    # Iterate over all rows: # TODO: Looping may not be the best...
    i = 0
    for row in input_df.itertuples(index=False):
        i += 1
        segment_ids = None

        # Extract values from CSV:
        site_id   = getattr(row, colname_site_id)
        subc_id   = getattr(row, "subc_id")
        basin_id  = getattr(row, "basin_id")
        reg_id    = getattr(row, "reg_id")

        # Stop if no site_id!
        if pd.isna(site_id):
            err_msg = "Missing site_id in row %s (subc_id=%s, basin_id=%s, reg_id=%s)" % (i, subc_id, basin_id, reg_id)
            LOGGER.error('(%s) %s' % (i, err_msg))
            raise ValueError(err_msg)
            continue

        # Ocean case...
        # As we do not return the site_ids, but only subc_ids with downstream path, there is no point in returning
        # an empty list for subc_id "NaN"... If we ever switch back to returning downstream ids for each site_id,
        # returning an empty list here make sense...
        if pd.isna(subc_id):
            # This can be the case if the point falls into the ocean...
            err_msg = "Cannot compute downstream ids due to missing subc_id at site %s" % site_id
            LOGGER.info('(%s) %s' % (i, err_msg))
            #segment_ids = []
            continue

        # Unexpected case...
        elif pd.isna(basin_id) or pd.isna(reg_id):
            LOGGER.error('TODO CAN THIS HAPPEN AT ALL? Can we have invalid reg_id/basin_id if we have no valid subc_id?')
            err_msg = "UNEXPECTED: Cannot compute downstream ids due to missing value(s) at site %s (subc_id=%s, basin_id=%s, reg_id=%s)" % \
                      (site_id, subc_id, outlet_id, basin_id, reg_id)
            LOGGER.error('(%s) %s' % (i, err_msg))
            raise ValueError(err_msg)

        else:

            # Cast to int if not NA! Otherwise the database query fails.
            if not pd.isna(subc_id):
                subc_id = int(subc_id)
            if not pd.isna(basin_id):
                basin_id = int(basin_id)
            if not pd.isna(reg_id):
                reg_id = int(reg_id)

            # Outlet has the id minus-basin!
            outlet_id = -basin_id

            if str(subc_id) in already_computed:
                LOGGER.debug('(%s) Not computing for site %s: Downstream segments already computed for subc_id %s' % (i, site_id, subc_id))
                continue

            else:
                # Now get the downstream ids from the database, for this point:
                # (We need to cast to int, as they come as decimal numbers...)
                subc_id = int(subc_id)
                outlet_id = int(outlet_id)
                basin_id = int(basin_id)
                reg_id = int(reg_id)
                LOGGER.debug('(%s) Computing downstream segments for site %s / for subc_id %s' % (i, site_id, subc_id))
                segment_ids = get_dijkstra_ids_one(conn, subc_id, outlet_id, reg_id, basin_id, silent=True)
                already_computed.add(str(subc_id))

        # Collect results for this row:
        if return_csv:
            # Output CSV:  We need to make one string out of the segment ids!
            # TODO: Separating the segment ids by "+" is not cool, but how to do it...
            segment_ids_str = "+".join([str(elem) for elem in segment_ids])
            everything.append([reg_id, basin_id, subc_id, segment_ids_str])
        elif return_json:
            everything.append({
                "subc_id": subc_id,
                "basin_id": basin_id,
                "reg_id": reg_id,
                "downstream_segments": segment_ids
            })

    # Finished collecting the results, now return JSON object:
    if return_csv:
        output_df = pd.DataFrame(everything, columns=['reg_id', 'basin_id', 'subc_id', 'downstream_segments'])
        return output_df
    elif return_json:
        output_json = {"downstream_segments_for_all_sites": everything}
        return output_json


def get_dijkstra_ids_many(conn, subc_ids, reg_id, basin_id):
    # INPUT: Set of subc_ids
    # OUTPUT: Route matrix (as JSON)

    LOGGER.debug('Compute distance matrix between %s subc_ids (in basin %s, region %s)' % (len(subc_ids), basin_id, reg_id))
    # TODO What if not in one basin?

    ### Construct SQL query:
    nodes = 'ARRAY[%s]' % ','.join(str(x) for x in subc_ids)
    query = 'SELECT start_vid, end_vid, edge'
    query += '''
    FROM pgr_dijkstra('
        SELECT
          subc_id AS id,
          subc_id AS source,
          target,
          length AS cost
          FROM hydro.stream_segments
          WHERE reg_id = {reg_id}
          AND basin_id = {basin_id}',
        {starts},
        {ends},
        directed := false);
    '''.format(reg_id = reg_id, basin_id = basin_id, starts = nodes, ends = nodes)
    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()
    LOGGER.log(logging.TRACE, "SQL query: %s" % query)
    # SQL query: SELECT edge FROM pgr_dijkstra(' SELECT   subc_id AS id,   subc_id AS source,   target,   length AS cost   FROM hydro.stream_segments   WHERE reg_id = 58   AND basin_id = 1294020', ARRAY[507294699,507282720,507199553,507332148,507290955], ARRAY[507294699,507282720,507199553,507332148,507290955], directed := false);
    
    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Construct result matrix:
    # TODO: JSON may not be the ideal type for returning a matrix!
    results_json = {}
    for start_id in subc_ids:
        results_json[str(start_id)] = {}
        for end_id in subc_ids:
            results_json[str(start_id)][str(end_id)] = [start_id] # TODO: check: Have to add start id?

    ### Iterating over the result rows:
    LOGGER.log(logging.TRACE, "Template for results: %s" % results_json)
    LOGGER.log(logging.TRACE, "Iterating over results...")
    while True:
        row = cursor.fetchone()
        if row is None: break
        
        # Collect all the ids along the paths:
        start_id  = row[0]
        end_id    = row[1]
        this_id   = row[2]
        if this_id == -1:
            pass
        else:
            results_json[str(start_id)][str(end_id)].append(this_id)
            LOGGER.log(logging.TRACE, 'Start %s to end %s, add this id %s' % (start_id, end_id, this_id))

    LOGGER.log(logging.TRACE, "Iterating over results... DONE.")
    #LOGGER.log(logging.TRACE, "JSON result: %s" % results_json) # quite big!

    return results_json


def get_dijkstra_distance_one(conn, start_subc_id, end_subc_id, reg_id, basin_id):
    # This simply returns one number!
    # INPUT: Start and end (subc_id)
    # OUTPUT: The distance (one number)!
    LOGGER.debug('Compute distance between subc_id %s and %s (in basin %s, region %s)' % (start_subc_id, end_subc_id, basin_id, reg_id))

    '''
    The distance between two points (507291111, 507292222) is just a number.

    Expressed like a matrix below, it would look like this (totally overdone):
    {
      '507291111': {'507291111': 0,     '507292222': 771.3, },
      '507292222': {'507291111': 771.3, '507292222': 0}

    '''

    ### Construct SQL query:
    query = 'SELECT edge, agg_cost'
    query += '''
    FROM pgr_dijkstra('
        SELECT
          subc_id AS id,
          subc_id AS source,
          target,
          length AS cost
          FROM hydro.stream_segments
          WHERE reg_id = {reg_id}
          AND basin_id = {basin_id}',
        {start_subc_id},
        {end_subc_id},
        directed := false);
    '''.format(reg_id = reg_id, basin_id = basin_id, start_subc_id = start_subc_id, end_subc_id = end_subc_id)
    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()
    LOGGER.log(logging.TRACE, "SQL query: %s" % query)

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Iterating over the result rows:
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
    # INPUT: Set of subc_ids
    # OUTPUT: Distance matrix (as JSON)

    LOGGER.debug('Compute distance matrix between %s subc_ids (in basin %s, region %s)' % (len(subc_ids), basin_id, reg_id))
    # TODO What if not in one basin?

    '''
    Example output: It's a JSONified matrix!
    {
      '507294699': {'507294699': 0,                 '507282720': 77136.30451583862, '507199553': 46228.42241668701, '507332148': 14313.99643707275, '507290955': 74875.18420028687},
      '507282720': {'507294699': 77136.30451583862, '507282720': 0,                 '507199553': 64695.14314651489, '507332148': 78088.08876419067, '507290955': 123218.3441696167},
      '507199553': {'507294699': 46228.42241668701, '507282720': 64695.14314651489, '507199553': 0,                 '507332148': 47180.20666503906, '507290955': 92310.46207046509},
      '507332148': {'507294699': 14313.99643707275, '507282720': 78088.08876419067, '507199553': 47180.20666503906, '507332148': 0,                 '507290955': 75826.96844863892},
      '507290955': {'507294699': 74875.18420028687, '507282720': 123218.3441696167, '507199553': 92310.46207046509, '507332148': 75826.96844863892, '507290955': 0}
    }
    '''


    ### Construct SQL query:
    nodes_start = 'ARRAY[%s]' % ','.join(str(x) for x in subc_ids_start)
    nodes_end = 'ARRAY[%s]' % ','.join(str(x) for x in subc_ids_end)
    query = 'SELECT edge, start_vid, end_vid, agg_cost'
    query += '''
    FROM pgr_dijkstra('
        SELECT
          subc_id AS id,
          subc_id AS source,
          target,
          length AS cost
          FROM hydro.stream_segments
          WHERE reg_id = {reg_id}
          AND basin_id = {basin_id}',
        {starts},
        {ends},
        directed := false);
    '''.format(reg_id = reg_id, basin_id = basin_id, starts = nodes_start, ends = nodes_end)
    query = query.replace("\n", " ")
    query = query.replace("    ", "")
    query = query.strip()
    LOGGER.log(logging.TRACE, "SQL query: %s" % query)
    # SQL query: SELECT edge FROM pgr_dijkstra(' SELECT   subc_id AS id,   subc_id AS source,   target,   length AS cost   FROM hydro.stream_segments   WHERE reg_id = 58   AND basin_id = 1294020', ARRAY[507294699,507282720,507199553,507332148,507290955], ARRAY[507294699,507282720,507199553,507332148,507290955], directed := false);
    
    ### Query database:
    cursor = conn.cursor()
    cursor.execute(query)

    ### Construct result matrix:
    # TODO: JSON may not be the ideal type for returning a matrix!
    results = {}
    for start_id in subc_ids_start:
        results[str(start_id)] = {}
        for end_id in subc_ids_end:
            results[str(start_id)][str(end_id)] = 0

    ### Iterating over the result rows:
    while True:
        row = cursor.fetchone()
        if row is None: break
        
        # We only look at the last edge of a path, as PostGIS returns agg_cost for us!
        if row[0] == -1: # if edge is -1...
            start_id  = row[1]
            end_id    = row[2]    
            agg_cost  = row[3]        
            results[str(start_id)][str(end_id)] = agg_cost
            LOGGER.log(logging.TRACE, 'Start %s to end %s, accumulated length %s' % 
                (start_id, end_id, agg_cost))

    return results


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

    ############################
    ### Define start and end ###
    ############################
    basin_id = 1292547
    reg_id = 58

    #lon_start = 9.937520027160646
    #lat_start = 54.69422745526058
    subc_id_start = 506251713
    #lon_end = 9.9217
    #lat_end = 54.6917
    subc_id_end = 506251712



#################
### One route ###
#################

if __name__ == "__main__" and True:

    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_one (just returns 5 ids)')
    res = get_dijkstra_ids_one(conn, subc_id_start, subc_id_end, reg_id, basin_id)
    print('RESULT: ROUTE:\n%s' % res)

    ####################
    ### Add Geometry ###
    ####################
    
    # If you then want geometries for this, use functions
    # "get_streamsegment_linestrings_feature_coll" and
    # "get_streamsegment_linestrings_geometry_coll"
    # from module aqua90m.geofresh.get_linestrings:
    import get_linestrings

    print('\nSTART PACKAGING IN GEOJSON...')

    # GeometryColl
    geom_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, res, basin_id, reg_id)
    print('\nRESULT (GeometryCollection):\n%s' % geom_coll)

    # Feature Coll
    feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(conn, res, basin_id, reg_id)
    print('\nRESULT (FeatureCollection/LineStrings):\n%s' % feature_coll)

    #####################
    ### One route,    ###
    ###  one distance ###
    #####################

    subc_id_start  = 507294699
    subc_id_end = 507282720
    basin_id = 1294020
    reg_id = 58

    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_one')
    res = get_dijkstra_ids_one(conn, subc_id_start, subc_id_end, reg_id, basin_id)
    print('RESULT: ROUTE: %s' % res) # just the list of ids

    print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_one')
    res = get_dijkstra_distance_one(conn, subc_id_start, subc_id_end, reg_id, basin_id)
    print('RESULT: DISTANCE: %s' % res) # just a number!


######################
### Many distances ###
######################

if __name__ == "__main__" and True:

    other1 = 507199553
    other2 = 507332148
    other3 = 507290955

    print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_many')
    res = get_dijkstra_distance_many(conn,
        [subc_id_start, subc_id_end, other1], reg_id, basin_id)
    print('RESULT: DISTANCE MATRIX: %s' % res)

    # This writes a lot of output!
    if False:
        print('\nSTART RUNNING FUNCTION: get_dijkstra_distance_many')
        res = get_dijkstra_distance_many(conn,
            [subc_id_start, subc_id_end, other1, other2, other3], reg_id, basin_id)
        print('RESULT: DISTANCE MATRIX: %s' % res)


###################
### Many routes ###
###################

if __name__ == "__main__" and True:

    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_many')
    res = get_dijkstra_ids_many(conn,
        [subc_id_start, subc_id_end, other1], reg_id, basin_id)
    print('RESULT: ROUTE MATRIX: %s' % res)

    # This writes a lot of output!
    if False:
        print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_many')
        res = get_dijkstra_ids_many(conn,
            [subc_id_start, subc_id_end, other1, other2, other3], reg_id, basin_id)
        print('RESULT: ROUTE MATRIX: %s' % res)


###################
### CSV version ###
###################

if __name__ == "__main__" and True:

    ## Input: dataframe, output dataframe, with site_id!
    input_df = pd.DataFrame(
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
            #['sea',  8.090485, 54.119322]
        ], columns=['site_id', 'lon', 'lat']
    )

    ## Now, for each row, get the ids!
    print('\nPREPARE RUNNING FUNCTION: get_dijkstra_ids_to_outlet_one_loop')
    import basic_queries
    temp_df = basic_queries.get_subcid_basinid_regid_for_all_1csv(conn, LOGGER, input_df, "lon", "lat", "site_id")
    print('\nSTART RUNNING FUNCTION: get_dijkstra_ids_to_outlet_one_loop')
    res = get_dijkstra_ids_to_outlet_one_loop(conn, temp_df, "site_id")
    print('RESULT: SEGMENTS IN DATAFRAME: %s' % res)
