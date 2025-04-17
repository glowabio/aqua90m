import json
import geomet.wkt
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

try:
    # If the package is installed in local python PATH:
    import aqua90m.geofresh.upstream_subcids as upstream_subcids
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)


def get_subcatchment_polygons_feature_coll(conn, subc_ids, basin_id, reg_id, add_subc_ids = False):

    # No upstream ids: (TODO: This should be caught earlier, probably):
    # Feature Collections can have empty array according to GeoJSON spec::
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
    if len(subc_ids) == 0:
        LOGGER.warning('No subc_ids. Cannot get their individual polygons.')
        return {
            "type": "FeatureCollection",
            "features": []
        }

    feature_list = _get_subcatchment_polygons(conn, subc_ids, basin_id, reg_id, make_features = True)

    feature_coll = {
        "type": "FeatureCollection",
        "features": feature_list,
        "reg_id": reg_id,
        "basin_id": basin_id
    }

    if add_subc_ids:
        feature_coll["subc_ids"] = subc_ids

    return feature_coll


def get_subcatchment_polygons_geometry_coll(conn, subc_ids, basin_id, reg_id):

    # No upstream ids: (TODO: This should be caught earlier, probably):
    # Geometry Collections can have empty array according to GeoJSON spec: ??? WIP TODO CHECK
    # https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
    if len(subc_ids) == 0:
        LOGGER.warning('No subc_ids. Cannot get their individual polygons.')
        geometry_coll = {
            "type": "GeometryCollection",
            "geometries": []
        }
        return geometry_coll

    geometry_list = _get_subcatchment_polygons(conn, subc_ids, basin_id, reg_id, make_features = False)

    return {
        "type": "GeometryCollection",
        "geometries": geometry_list
    }


def _get_subcatchment_polygons(conn, subc_ids, basin_id, reg_id, make_features = True):
    # Private function. Should not be used outside this module, as it returns incomplete GeoJSON.

    LOGGER.debug('Querying for polygons for %s subc_ids...' % len(subc_ids))

    upstream_subcids.too_many_upstream_catchments(len(subc_ids), 'individual polygons')


    ### Define query:
    relevant_ids = ", ".join([str(elem) for elem in subc_ids])
    # e.g. 506250459, 506251015, 506251126, 506251712
    query = '''
    SELECT ST_AsText(geom), subc_id
    FROM sub_catchments
    WHERE subc_id IN ({relevant_ids})
    AND basin_id = {basin_id}
    AND reg_id = {reg_id}
    '''.format(relevant_ids = relevant_ids, basin_id = basin_id, reg_id = reg_id)

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
    items = []
    while (True):
        row = cursor.fetchone()
        if row is None:
            break

        # Create GeoJSON geometry from each result row:
        geometry = None
        if row[0] is not None:
            geometry = geomet.wkt.loads(row[0])
        else:
            LOGGER.error('Subcatchment %s has no polygon!' % row[1]) # for example: 506469602

        if make_features:
            items.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "subc_id": row[1]
                }
            })
        else:
            items.append(geometry)

    return items






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
    LOGGER.log(logging.TRACE, 'Connecting to database... DONE.')

    ####################
    ### Run function ###
    ####################
    one_subc_id = 506250459
    subc_ids = [506250459, 506251015, 506251126, 506251712]
    basin_id = 1292547
    reg_id = 58

    # GeometryCollection (for several geometries):
    print('\nSTART RUNNING FUNCTION: get_subcatchment_polygons_geometry_coll')
    res = get_subcatchment_polygons_geometry_coll(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)

    # FeatureCollection (for several features):
    print('\nSTART RUNNING FUNCTION: get_subcatchment_polygons_feature_coll')
    res = get_subcatchment_polygons_feature_coll(conn, subc_ids, basin_id, reg_id)
    print('RESULT:\n%s' % res)
    
    # For just one Geometry:
    print('\nSTART RUNNING FUNCTION: get_subcatchment_polygons_geometry_coll')
    res = get_subcatchment_polygons_geometry_coll(conn, [one_subc_id], basin_id, reg_id)
    res = res["geometries"][0]
    print('RESULT:\n%s' % res)

    # For just one Feature:
    print('\nSTART RUNNING FUNCTION: get_subcatchment_polygons_feature_coll')
    res = get_subcatchment_polygons_feature_coll(conn, [one_subc_id], basin_id, reg_id)
    res = res["features"][0]
    print('RESULT:\n%s' % res)
