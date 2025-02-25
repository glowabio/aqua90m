import json
import logging
import geomet.wkt
LOGGER = logging.getLogger(__name__)

try:
    from ..utils import extent_helpers as extent_helpers
except ModuleNotFoundError:
    import pygeoapi.process.aqua90m.utils.extent_helpers as extent_helpers

def get_reg_id(conn, lon, lat):

    extent_helpers.check_outside_europe(lon, lat) # may raise ValueError!

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
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None: # Ocean case: 
        error_message      = 'No region id found for lon %s, lat %s! Is this in the ocean?' % (lon, lat)
        user_error_message = 'No result found for lon %s, lat %s! Is this in the ocean?' % (round(lon, 3), round(lat, 3))
        LOGGER.error(error_message)
        raise ValueError(user_error_message)

    else:
        reg_id = row[0]

    return reg_id


def get_subc_id_basin_id(conn, lon, lat, reg_id):

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
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results:
    row = cursor.fetchone()
    if row is None: # Ocean case:     
        error_message = 'No subc_id and basin_id. This should have been caught before. Does this latlon fall into the ocean?'
        user_error_message = 'No result (basin, sub_catchment) found for lon %s, lat %s! Is this in the ocean?' % (lon, lat)
        LOGGER.error(error_message)
        raise ValueError(user_error_message)
    else:
        subc_id = row[0]
        basin_id = row[1]

    return subc_id, basin_id 


def get_basin_id_reg_id(conn, subc_id):

    ### Define query:
    query = """
    SELECT basin_id, reg_id
    FROM sub_catchments
    WHERE subc_id = {given_subc_id}
    """.format(given_subc_id = subc_id)
    query = query.replace("\n", " ")

    ### Query database:
    cursor = conn.cursor()
    LOGGER.debug('Querying database...')
    cursor.execute(query)
    LOGGER.debug('Querying database... DONE.')

    ### Get results and construct GeoJSON:
    row = cursor.fetchone()
    if row is None:
        error_message = 'No basin_id and reg_id found for subc_id %s!' % subc_id
        LOGGER.error(error_message)
        raise ValueError(error_message)
    else:
        basin_id = row[0]
        reg_id = row[1]

    return basin_id, reg_id


def get_subc_id_basin_id_reg_id(conn, LOGGER, lon = None, lat = None, subc_id = None):
    # This is a wrapper

    # If user provided subc_id, then use it!
    if subc_id is not None:
        LOGGER.debug('Getting subcatchment, region and basin id for subc_id: %s' % subc_id)
        subc_id, basin_id, reg_id = get_subc_id_basin_id_reg_id_from_subc_id(conn, subc_id, LOGGER)

    # Standard case: User provided lon and lat!
    elif lon is not None and lat is not None:
            LOGGER.debug('Getting subcatchment, region and basin id for lon, lat: %s, %s' % (lon, lat))
            lon = float(lon)
            lat = float(lat)
            subc_id, basin_id, reg_id = get_subc_id_basin_id_reg_id_from_lon_lat(conn, lon, lat, LOGGER)
    else:
        error_message = 'Lon and lat (or subc_id) have to be provided! Lon: %s, lat: %s, subc_id %s' % (lon, lat, subc_id)
        raise ValueError(error_message)

    return subc_id, basin_id, reg_id


def get_subc_id_basin_id_reg_id_from_lon_lat(conn, lon, lat, LOGGER):

    # Get reg_id
    reg_id = get_reg_id(conn, lon, lat)
    
    if reg_id is None: # Might be in the ocean!
        error_message = "Caught an error that should have been caught before! (reg_id = None)!"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    # Get basin_id, subc_id
    subc_id, basin_id = get_subc_id_basin_id(conn, lon, lat, reg_id)
    # NameError: name 'get_subc_id_basin_id' is not defined. Did you mean: 'get_subc_id_basin_id_reg_id'?

    
    if basin_id is None:
        LOGGER.error('No basin_id id found for lon %s, lat %s !' % (lon, lat))
    
    LOGGER.debug('... Subcatchment has subc_id %s, basin_id %s, reg_id %s.' % (subc_id, basin_id, reg_id))

    return subc_id, basin_id, reg_id


def get_subc_id_basin_id_reg_id_from_subc_id(conn, subc_id, LOGGER):

    # Get basin_id, reg_id
    basin_id, reg_id = get_basin_id_reg_id(conn, subc_id)
    
    if reg_id is None:
        error_message = 'No reg_id id found for subc_id %s' % subc_id
        LOGGER.error(error_message)
        raise ValueError(error_message)
    
    if basin_id is None:
        error_message = 'No basin_id id found for subc_id %s' % subc_id
        LOGGER.error(error_message)
        raise ValueError(error_message)
    
    LOGGER.debug('Subcatchment has subc_id %s, basin_id %s, reg_id %s.' % (subc_id, basin_id, reg_id))

    return subc_id, basin_id, reg_id


#################################
### for many points at a time ###
#################################

def get_subc_id_basin_id_reg_id_for_all(conn, LOGGER, points_geojson):

    dummy = None
    everything = {}
    basin_ids = []
    reg_ids = []
    subc_ids = []
    num = len(points_geojson['geometries'])

    # Iterate over points and call "get_subc_id_basin_id_reg_id" for each point:
    # TODO: This is not super efficient, but the quickest to implement :)
    for point in points_geojson['geometries']:
        lon, lat = point['coordinates']
        LOGGER.debug('Getting subcatchment for lon, lat: %s, %s' % (lon, lat))
        subc_id, basin_id, reg_id = get_subc_id_basin_id_reg_id(
            conn, LOGGER, lon, lat, dummy)
        LOGGER.debug('TYPES: %s %s %s' % (type(reg_id), type(basin_id), type(subc_id)))
        LOGGER.debug('VALUES: %s %s %s' % (reg_id, basin_id, subc_id))
        reg_ids.append(str(reg_id))
        basin_ids.append(str(basin_id))
        subc_ids.append(str(subc_id))

        if not reg_id in everything:
            everything[reg_id] = {basin_id: {subc_id: [point]}}
        else:
            if not basin_id in everything[reg_id]:
                everything[reg_id][basin_id] = {subc_id: [point]}
            else:
                if not subc_id in everything[reg_id][basin_id]:
                    everything[reg_id][basin_id][subc_id] = [point]
                else:
                    everything[reg_id][basin_id][subc_id].append(point)


    # Extensive logging...
    LOGGER.info('Of %s points, ...')

    # Stats reg_id...
    if len(set(reg_ids)) == 1:
        LOGGER.info('... all %s points fall into regional unit with reg_id %s' % (num, reg_ids[0]))
    else:
        reg_id_counts = {reg_id: reg_ids.count(reg_id) for reg_id in reg_ids}
        for reg_id in set(reg_ids):
            LOGGER.info('... %s points fall into regional unit with reg_id %s' % (reg_id_counts[reg_id], reg_id))

    # Stats basin_id...
    if len(set(basin_ids)) == 1:
        LOGGER.info('... all %s points fall into drainage basin with basin_id %s' % (num, basin_ids[0]))
    else:
        #basin_id_counts = {basin_id:basin_ids.count(i) for basin_id in basin_ids}
        basin_id_counts = {basin_id: basin_ids.count(basin_id) for basin_id in basin_ids}
        for basin_id in set(basin_ids):
            LOGGER.info('... %s points fall into drainage basin with basin_id %s' % (basin_id_counts[basin_id], basin_id))

    # Stats subc_id...
    if len(set(subc_ids)) == 1:
        LOGGER.info('... all %s points fall into subcatchment with subc_id %s' % (num, subc_ids[0]))
    else:
        #subc_id_counts = {subc_id:subc_ids.count(i) for subc_id in subc_ids}
        subc_id_counts = {subc_id: subc_ids.count(subc_id) for subc_id in subc_ids}
        for subc_id in set(subc_ids):
            LOGGER.info('... %s points fall into subcatchment with subc_id %s' % (subc_id_counts[subc_id], subc_id))

    # Note: This is not GeoJSON (on purpose), as we did not look for geometry yet.
    output = {
        "subc_ids":   ', '.join(str(i) for i in set(subc_ids)),
        "region_ids": ', '.join(str(i) for i in set(reg_ids)),
        "basin_ids":  ', '.join(str(i) for i in set(basin_ids)),
        "everything": everything
    }

    return output



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
    ### Run function ###
    ####################

    print('\nSTART RUNNING FUNCTION: get_reg_id')
    lon = 9.931555
    lat = 54.695070
    res = get_reg_id(conn, lon, lat)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: get_subc_id_basin_id')
    lon = 9.931555
    lat = 54.695070
    reg_id = 58
    res = get_subc_id_basin_id(conn, lon, lat, reg_id)
    print('RESULT: %s %s' % (res[0], res[1]))

    print('\nSTART RUNNING FUNCTION: get_basin_id_reg_id')
    one_subc_id = 506250459
    res = get_basin_id_reg_id(conn, one_subc_id)
    print('RESULT: %s %s' % (res[0], res[1]))

    print('\nSTART RUNNING FUNCTION: get_subc_id_basin_id_reg_id (using subc_id)')
    one_subc_id = 506250459
    res = get_subc_id_basin_id_reg_id(conn, LOGGER, lon = None, lat = None, subc_id = one_subc_id)
    print('RESULT: %s %s %s' % (res[0], res[1], res[2]))

    print('\nSTART RUNNING FUNCTION: get_subc_id_basin_id_reg_id (using lon, lat)')
    lon = 9.931555
    lat = 54.695070
    res = get_subc_id_basin_id_reg_id(conn, LOGGER, lon = lon, lat = lat, subc_id = None)
    print('RESULT: %s %s %s' % (res[0], res[1], res[2]))

    print('\nSTART RUNNING FUNCTION: get_subc_id_basin_id_reg_id_for_all (using Multipoint)')
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
    res = get_subc_id_basin_id_reg_id_for_all(conn, LOGGER, points_geojson)
    print('RESULT:\n%s' % res)

