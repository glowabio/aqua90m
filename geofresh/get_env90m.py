import json
import geomet.wkt
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

try:
    # If the package is installed in local python PATH:
    import aqua90m.utils.exceptions as exc
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.exceptions as exc
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+'. If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)

'''
# Database tables:
stats_flow1k (mean, sd, min, max)
    flow_ltm
    flow_ltsd
stats_climate (mean, sd, min, max)
    bio1
    ...
    bio19
TODO: stats_clim_future (mean, sd, min, max)(same names as stats_climate!)
stats_landuse (just the values)
    c10
    ...
    c220
stats_soil (mean, sd, min, max)
stats_standardized (just the value)
stats_topo (just the value) (OMG many...)

# All the columns:

DONE: stats_landuse:
 subc_id | c10 | c20 | c30 | c40 | c50 | c60 | c70 | c80 | c90 | c100 | c110 | c120 | c130 | c140 | c150 | c160 | c170 | c180 | c190 | c200 | c210 | c220 | year | reg_id 

DONE: stats_flow1k:
 subc_id | flow_ltm_min | flow_ltm_max | flow_ltm_mean | flow_ltm_sd | flow_ltsd_min | flow_ltsd_max | flow_ltsd_mean | flow_ltsd_sd | reg_id 

DONE: status_climate:
 subc_id | bio1_min | bio1_max | bio1_mean | bio1_sd | bio2_min | bio2_max | bio2_mean | bio2_sd | bio3_min | bio3_max | bio3_mean | bio3_sd | bio4_min | bio4_max | bio4_mean | bio4_sd | bio5_min | bio5_max | bio5_mean | bio5_sd | bio6_min | bio6_max | bio6_mean | bio6_sd | bio7_min | bio7_max | bio7_mean | bio7_sd | bio8_min | bio8_max | bio8_mean | bio8_sd | bio9_min | bio9_max | bio9_mean | bio9_sd | bio10_min | bio10_max | bio10_mean | bio10_sd | bio11_min | bio11_max | bio11_mean | bio11_sd | bio12_min | bio12_max | bio12_mean | bio12_sd | bio13_min | bio13_max | bio13_mean | bio13_sd | bio14_min | bio14_max | bio14_mean | bio14_sd | bio15_min | bio15_max | bio15_mean | bio15_sd | bio16_min | bio16_max | bio16_mean | bio16_sd | bio17_min | bio17_max | bio17_mean | bio17_sd | bio18_min | bio18_max | bio18_mean | bio18_sd | bio19_min | bio19_max | bio19_mean | bio19_sd | reg_id 

TODO: stats_clim_future:
 subc_id | reg_id | timeperiod | model | ssp | bio1_min | bio1_max | bio1_mean | bio1_sd | bio2_min | bio2_max | bio2_mean | bio2_sd | bio3_min | bio3_max | bio3_mean | bio3_sd | bio4_min | bio4_max | bio4_mean | bio4_sd | bio5_min | bio5_max | bio5_mean | bio5_sd | bio6_min | bio6_max | bio6_mean | bio6_sd | bio7_min | bio7_max | bio7_mean | bio7_sd | bio8_min | bio8_max | bio8_mean | bio8_sd | bio9_min | bio9_max | bio9_mean | bio9_sd | bio10_min | bio10_max | bio10_mean | bio10_sd | bio11_min | bio11_max | bio11_mean | bio11_sd | bio12_min | bio12_max | bio12_mean | bio12_sd | bio13_min | bio13_max | bio13_mean | bio13_sd | bio14_min | bio14_max | bio14_mean | bio14_sd | bio15_min | bio15_max | bio15_mean | bio15_sd | bio16_min | bio16_max | bio16_mean | bio16_sd | bio17_min | bio17_max | bio17_mean | bio17_sd | bio18_min | bio18_max | bio18_mean | bio18_sd | bio19_min | bio19_max | bio19_mean | bio19_sd 

DONE: stats_soil:
 subc_id | awcts_min | awcts_max | awcts_mean | awcts_sd | clyppt_min | clyppt_max | clyppt_mean | clyppt_sd | sndppt_min | sndppt_max | sndppt_mean | sndppt_sd | sltppt_min | sltppt_max | sltppt_mean | sltppt_sd | wwp_min | wwp_max | wwp_mean | wwp_sd | texmht_min | texmht_max | texmht_mean | texmht_sd | orcdrc_min | orcdrc_max | orcdrc_mean | orcdrc_sd | phihox_min | phihox_max | phihox_mean | phihox_sd | bldfie_min | bldfie_max | bldfie_mean | bldfie_sd | cecsol_min | cecsol_max | cecsol_mean | cecsol_sd | crfvol_min | crfvol_max | crfvol_mean | crfvol_sd | acdwrb_min | acdwrb_max | acdwrb_mean | acdwrb_sd | bdricm_min | bdricm_max | bdricm_mean | bdricm_sd | bdrlog_min | bdrlog_max | bdrlog_mean | bdrlog_sd | histpr_min | histpr_max | histpr_mean | histpr_sd | slgwrb_min | slgwrb_max | slgwrb_mean | slgwrb_sd | reg_id 

stats_topo:
 subc_id | shreve | scheidegger | length | stright | sinusoid | cum_length | flow_accum | out_dist | source_elev | outlet_elev | elev_drop | out_drop | gradient | strahler | horton | hack | topo_dim | elev_min | elev_max | elev_mean | elev_sd | flow_min | flow_max | flow_mean | flow_sd | flowpos_min | flowpos_max | flowpos_mean | flowpos_sd | slope_curv_max_dw_cel_min | slope_curv_max_dw_cel_max | slope_curv_max_dw_cel_mean | slope_curv_max_dw_cel_sd | slope_curv_min_dw_cel_min | slope_curv_min_dw_cel_max | slope_curv_min_dw_cel_mean | slope_curv_min_dw_cel_sd | slope_elv_dw_cel_min | slope_elv_dw_cel_max | slope_elv_dw_cel_mean | slope_elv_dw_cel_sd | slope_grad_dw_cel_min | slope_grad_dw_cel_max | slope_grad_dw_cel_mean | slope_grad_dw_cel_sd | stream_dist_up_near_min | stream_dist_up_near_max | stream_dist_up_near_mean | stream_dist_up_near_sd | stream_dist_up_farth_min | stream_dist_up_farth_max | stream_dist_up_farth_mean | stream_dist_up_farth_sd | stream_dist_dw_near_min | stream_dist_dw_near_max | stream_dist_dw_near_mean | stream_dist_dw_near_sd | outlet_dist_dw_basin_min | outlet_dist_dw_basin_max | outlet_dist_dw_basin_mean | outlet_dist_dw_basin_sd | outlet_dist_dw_scatch_min | outlet_dist_dw_scatch_max | outlet_dist_dw_scatch_mean | outlet_dist_dw_scatch_sd | stream_dist_proximity_min | stream_dist_proximity_max | stream_dist_proximity_mean | stream_dist_proximity_sd | stream_diff_up_near_min | stream_diff_up_near_max | stream_diff_up_near_mean | stream_diff_up_near_sd | stream_diff_up_farth_min | stream_diff_up_farth_max | stream_diff_up_farth_mean | stream_diff_up_farth_sd | stream_diff_dw_near_min | stream_diff_dw_near_max | stream_diff_dw_near_mean | stream_diff_dw_near_sd | outlet_diff_dw_basin_min | outlet_diff_dw_basin_max | outlet_diff_dw_basin_mean | outlet_diff_dw_basin_sd | outlet_diff_dw_scatch_min | outlet_diff_dw_scatch_max | outlet_diff_dw_scatch_mean | outlet_diff_dw_scatch_sd | channel_grad_dw_seg_min | channel_grad_dw_seg_max | channel_grad_dw_seg_mean | channel_grad_dw_seg_sd | channel_grad_up_seg_min | channel_grad_up_seg_max | channel_grad_up_seg_mean | channel_grad_up_seg_sd | channel_grad_up_cel_min | channel_grad_up_cel_max | channel_grad_up_cel_mean | channel_grad_up_cel_sd | channel_curv_cel_min | channel_curv_cel_max | channel_curv_cel_mean | channel_curv_cel_sd | channel_elv_dw_seg_min | channel_elv_dw_seg_max | channel_elv_dw_seg_mean | channel_elv_dw_seg_sd | channel_elv_up_seg_min | channel_elv_up_seg_max | channel_elv_up_seg_mean | channel_elv_up_seg_sd | channel_elv_up_cel_min | channel_elv_up_cel_max | channel_elv_up_cel_mean | channel_elv_up_cel_sd | channel_elv_dw_cel_min | channel_elv_dw_cel_max | channel_elv_dw_cel_mean | channel_elv_dw_cel_sd | channel_dist_dw_seg_min | channel_dist_dw_seg_max | channel_dist_dw_seg_mean | channel_dist_dw_seg_sd | channel_dist_up_seg_min | channel_dist_up_seg_max | channel_dist_up_seg_mean | channel_dist_up_seg_sd | channel_dist_up_cel_min | channel_dist_up_cel_max | channel_dist_up_cel_mean | channel_dist_up_cel_sd | spi_min | spi_max | spi_mean | spi_sd | sti_min | sti_max | sti_mean | sti_sd | cti_min | cti_max | cti_mean | cti_sd | drwal_old | reg_id 

stats_standardized:
 bio1 | bio2 | bio3 | bio4 | bio5 | bio6 | bio7 | bio8 | bio9 | bio10 | bio11 | bio12 | bio13 | bio14 | bio15 | bio16 | bio17 | bio18 | bio19 | c10 | c20 | c30 | c40 | c50 | c60 | c70 | c80 | c90 | c100 | c110 | c120 | c130 | c140 | c150 | c160 | c170 | c180 | c190 | c200 | c210 | c220 | awcts | clyppt | sndppt | sltppt | wwp | orcdrc | phihox | bldfie | cecsol | crfvol | acdwrb | bdricm | bdrlog | histpr | slgwrb | elev | flowpos | slope_curv_max_dw_cel | slope_curv_min_dw_cel | slope_elv_dw_cel | slope_grad_dw_cel | stream_dist_up_near | stream_dist_up_farth | stream_dist_dw_near | outlet_dist_dw_basin | outlet_dist_dw_scatch | stream_dist_proximity | stream_diff_up_near | stream_diff_up_farth | stream_diff_dw_near | outlet_diff_dw_basin | outlet_diff_dw_scatch | channel_grad_dw_seg | channel_grad_up_seg | channel_grad_up_cel | channel_curv_cel | channel_elv_dw_seg | channel_elv_up_seg | channel_elv_up_cel | channel_elv_dw_cel | channel_dist_dw_seg | channel_dist_up_seg | channel_dist_up_cel | strahler | shreve | horton | hack | topo_dim | length | stright | sinusoid | cum_length | out_dist | source_elev | outlet_elev | elev_drop | out_drop | gradient | spi | sti | cti | subc_id | reg_id | flow_ltm | flow_ltsd 


Input: Two cases:
* get_upstream_subcids
* get_local_subcids_plural
'''

def get_env90m_variables_by_subcid(conn, subc_ids, reg_id, variables):

    ### Define query:
    '''
    Example query:
    geofresh_data=> SELECT subc_id, reg_id, bio1_mean, bio1_min, bio1_max, bio1_sd FROM stats_climate WHERE subc_id IN (506250459, 506251015, 506251126, 506251712) AND reg_id = 58;


    # TODO ASK: Use basin_id?
    # Let user decide which statistic?
    # Use partition???
    '''

    # Join the ids
    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712

    # Which variables come from which table?
    tables_variables = {}
    for var in variables:

        # Find table name for this var:
        soil_vars = set(['awcts', 'sndppt', 'sltppt', 'bldfie', 'histpr', 'orcdrc',
                         'crfvol', 'acdwrb', 'phihox', 'bdricm', 'cecsol',
                         'clyppt', 'bdrlog', 'slgwrb', 'wwp', 'texmht'])
        topo_vars = [
            'channel_curv_cel',
            'channel_dist_dw_seg',
            'channel_dist_up_cel',
            'channel_dist_up_seg',
            'channel_elv_dw_cel',
            'channel_elv_dw_seg',
            'channel_elv_up_cel',
            'channel_elv_up_seg',
            'channel_grad_dw_seg',
            'channel_grad_up_cel',
            'channel_grad_up_seg',
            'cti',
            'cum_length',
            'drwal_old',
            'elev',
            'elev_drop',
            'flow',
            'flow_accum',
            'flowpos',
            'gradient',
            'hack',
            'horton',
            'length',
            'out_dist',
            'out_drop',
            'outlet_diff_dw_basin',
            'outlet_diff_dw_scatch',
            'outlet_dist_dw_basin',
            'outlet_dist_dw_scatch',
            'outlet_elev',
            'scheidegger',
            'shreve',
            'sinusoid',
            'slope_curv_max_dw_cel',
            'slope_curv_min_dw_cel',
            'slope_elv_dw_cel',
            'slope_grad_dw_cel',
            'source_elev',
            'spi',
            'sti',
            'strahler',
            'stream_diff_dw_near',
            'stream_diff_up_farth',
            'stream_diff_up_near',
            'stream_dist_dw_near',
            'stream_dist_proximity',
            'stream_dist_up_farth',
            'stream_dist_up_near',
            'stright',
            'topo_dim'
        ]
        if var == "flow_ltm" or var == "flow_ltsd":
            table_name = "stats_flow1k"
        elif var in soil_vars:
            table_name = "stats_soil"
        elif var in topo_vars:
            table_name = "stats_topo"
        elif var.startswith("bio"):
            table_name = "stats_climate" # or "stats_clim_future"!
            LOGGER.warning('These variable could also be table stats_clim_future, how to distinguish?')
        elif var.startswith("c"):
            try:
                int(var[1:])
                table_name = "stats_landuse"
            except ValueError as e:
                pass
        else:
            err_msg = "WIP. Variable mistyped or not implemented: %s" % var
            LOGGER.warning(err_msg)
            raise exc.UserInputException(err_msg)

        # Collect var and table name:
        if not table_name in tables_variables.keys():
            tables_variables[table_name] = [var]
        else:
            tables_variables[table_name].append(var)


    #if len(tables_variables.keys()) > 1:
    #    err_msg = "WIP. You stated variables from several tables (%s), can only treat one table so far." % tables_variables.keys()
    #    LOGGER.warn(err_msg)
    #    raise exc.UserInputException(err_msg)


    ## Iterate over all tables, query:
    json_result = {}
    LOGGER.debug("We will query %s tables: %s" % (len(tables_variables), tables_variables))
    for table_name, variables in tables_variables.items():

        ## Which statistics exist for that table?
        if table_name == "stats_landuse":
            possible_statistics = []
        else:
            possible_statistics = ["mean", "sd", "min", "max"]

        ## Generate and join the column names (with statistics or not)
        column_names_list = []
        if len(possible_statistics) == 0:
            column_names_list = variables
        else:
            for variable in variables:
                # Special case stats_topo: Some variables have statistics, some don't
                # TODO: not a good solution!!!
                if table_name == "stats_topo" and variable in ['shreve', 'hack',
                    'scheidegger', 'length', 'stright', 'sinusoid', 'cum_length',
                    'flow_accum', 'out_dist', 'source_elev', 'outlet_elev',
                    'elev_drop', 'out_drop', 'gradient', 'strahler', 'horton',
                    'topo_dim', 'drwal_old']:
                    column_names_list.append(variable)
                else:
                    for statistic in possible_statistics:
                        column_names_list.append(variable+"_"+statistic)
        column_names_str = ", ".join([str(elem) for elem in column_names_list])

        ## Create SQL query:
        LOGGER.log(logging.TRACE, "Now querying table %s for columns %s" % (table_name, column_names_list))
        query = '''
        SELECT 
        subc_id, {column_names}
        FROM hydro.{table_name}
        WHERE subc_id IN ({relevant_ids})
        AND reg_id = {reg_id}
        '''.format(column_names = column_names_str, relevant_ids = relevant_ids,
                   table_name = table_name, reg_id = reg_id)
        query = query.replace("\n", " ")

        ### Query database:
        cursor = conn.cursor()
        LOGGER.log(logging.TRACE, 'Querying database...')
        cursor.execute(query)
        LOGGER.log(logging.TRACE, 'Querying database... DONE.')

        ### Get results and construct JSON:
        LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing JSON...')

        ## Iterate over all result rows / all subcatchments 
        while (True):
            row = cursor.fetchone()
            if row is None:
                break

            subc_id = row[0]
            LOGGER.log(logging.TRACE, "Subcatchment: %s" % subc_id)
            LOGGER.log(logging.TRACE, "Result row: %s - %s" % (column_names_str, row))
            if not str(subc_id) in json_result:
                json_result[str(subc_id)] = {}

            ## The next indices are the other variables
            row_index = 1
            for col_name in column_names_list:
                LOGGER.log(logging.TRACE, "   Col %s: %s" % (col_name, row[row_index]))
                json_result[str(subc_id)][col_name] = row[row_index]
                row_index += 1


    LOGGER.log(logging.TRACE, 'Overall result: %s' % json_result)
    return json_result




if __name__ == "__main__":

    # Logging
    verbose = True
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

    try:
        # If the package is properly installed, thus it is findable by python on PATH:
        import aqua90m.utils.exceptions as exc
    except ModuleNotFoundError:
        # If we are calling this script from the aqua90m parent directory via
        # "python aqua90m/geofresh/basic_queries.py", we have to make it available on PATH:
        import sys, os
        sys.path.append(os.getcwd())
        import aqua90m.utils.exceptions as exc

    ####################
    ### Run function ###
    ####################
    subc_ids = [506250459, 506251015, 506251126, 506251712]
    #basin_id = 1292547
    reg_id = 58

    print('\nSTART RUNNING FUNCTION: get_streamsegment_linestrings_geometry_coll')
    variables = ["bio1", "bio7"]
    res = get_env90m_variables_by_subcid(conn, subc_ids, reg_id, variables)
    print('RESULT:\n%s' % res)

    print('\nSTART RUNNING FUNCTION: get_streamsegment_linestrings_geometry_coll')
    variables = ["bio1", "bio7", "c20", "flow_ltm"]
    res = get_env90m_variables_by_subcid(conn, subc_ids, reg_id, variables)
    print('RESULT:\n%s' % res)

    print('\nTEST CUSTOM EXCEPTION: get_env90m_variables_by_subcid...')
    try:
        res = get_env90m_variables_by_subcid(conn, subc_ids, reg_id, ['bla', 'bli', 'blu'])
        raise RuntimeError('Should not reach here!')
    except exc.UserInputException as e:
        print('RESULT: Proper exception, saying: %s' % e)
