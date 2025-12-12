import json
import geomet.wkt
import geomet.wkb
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


def get_outlet_subcids_in_polygon(conn, polygon_geojson, min_strahler=1):
    LOGGER.debug('**************************************************')
    LOGGER.debug('Querying for outlettttttttttttttttttttts in for a polygon...')

    ### Stringify GeoJSON:
    polygon_geojson_str = f'{polygon_geojson}'
    polygon_geojson_str = polygon_geojson_str.replace("'", '"')

    ### Define query:
    query = '''
    SELECT subc_id, basin_id
    FROM stream_segments
    WHERE target = -basin_id
    AND strahler >= {min_strahler}
    AND ST_WITHIN(stream_segments.geom, ST_GeomFromGeoJSON(
        '{polygon_geojson}'
    ));
    '''.format(min_strahler=min_strahler, polygon_geojson=polygon_geojson_str)

    LOGGER.info(f'SQL QUERY : \n{query}\n')
    #LOGGER.info(f'SQL QUERY PRE : \n{query}\n')
    #query = query.replace("'", '"')
    #LOGGER.info(f'SQL QUERY POST: \n{query}\n')

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
    subcids = {}
    while (True):
        row = cursor.fetchone()
        if row is None:
            break

        subc_id = None
        basin_id = None
        if row[0] is not None:
          subc_id = row[0]
        if row[1] is not None:
          basin_id = row[1]

        subcids[basin_id] = subc_id

    return subcids




def get_outlet_streamsegments_in_polygon(conn, polygon_geojson, min_strahler=1):
    LOGGER.debug('**************************************************')
    LOGGER.debug('Querying for outlettttttttttttttttttttts in for a polygon...')

    ### Stringify GeoJSON:
    polygon_geojson_str = f'{polygon_geojson}'
    polygon_geojson_str = polygon_geojson_str.replace("'", '"')

    ### Define query:
    query = '''
    SELECT subc_id, basin_id, geom
    FROM stream_segments
    WHERE target = -basin_id
    AND strahler >= {min_strahler}
    AND ST_WITHIN(stream_segments.geom, ST_GeomFromGeoJSON(
        '{polygon_geojson}'
    ));
    '''.format(min_strahler=min_strahler, polygon_geojson=polygon_geojson_str)
    LOGGER.info(f'SQL QUERY: \n{query}\n')
    #LOGGER.info(f'SQL QUERY PRE : \n{query}\n')
    #query = query.replace("'", '"')
    #LOGGER.info(f'SQL QUERY POST: \n{query}\n')

    ### Query database:
    cursor = conn.cursor()
    LOGGER.log(logging.TRACE, 'Querying database...')
    cursor.execute(query)
    LOGGER.log(logging.TRACE, 'Querying database... DONE.')

    ### Get results and construct GeoJSON:
    LOGGER.log(logging.TRACE, 'Iterating over the result rows, constructing GeoJSON...')
    featurecoll = {
        "type": "FeatureCollection",
        "features": []
    }
    while (True):
        row = cursor.fetchone()
        if row is None:
            break

        subc_id = None
        basin_id = None
        geometry = None

        if row[0] is not None:
            subc_id = row[0]

        if row[1] is not None:
          basin_id = row[1]

        if row[2] is not None:
            LOGGER.debug(f'GEOM ROW {row[2]}')
            try:
                #geometry = geomet.wkt.loads(row[2])
                geometry = geomet.wkb.loads(bytes.fromhex(row[2]))
                LOGGER.debug(f'GEOMETRY {geometry}')
            except ValueError as e:
                err_msg = f'Failed to parse geometry for stream segment {subc_id} (in basin {basin_id}): {e}'
                LOGGER.error(err_msg)
                raise ValueError(err_msg) from e

        else:
            # Geometry errors that happen when two segments flow into one outlet (Vanessa, 17 June 2024)
            # For example, subc_id 506469602, when routing from 507056424 to outlet -1294020
            LOGGER.error(f'Subcatchment {subc_id} has no geometry!') # for example: 506469602
            # Features with empty geometries:
            # A geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

        feature = {
            "type": "Feature",
            "properties": {
                "subc_id": subc_id,
                "basin_id": basin_id,
                "outlet_of_basin": basin_id
            },
            "geometry": geometry
        }


        featurecoll["features"].append(feature)

    return featurecoll



if __name__ == '__main__':

    polygon_geojson = '''
    {
        "type": "Polygon",
        "coordinates": [
          [
            [ 24.99422594742927, 60.1221882389213],
            [ 24.99422594742927, 60.2873916947333],
            [ 24.524039063708727, 60.287391694733],
            [ 24.524039063708727, 60.122188238921],
            [ 24.99422594742927, 60.1221882389213]
          ]
        ]
    }
    '''
    LOGGER.info(f'Input: {polygon_geojson}')

    min_strahler = 5
    LOGGER.info(f'Minimum Strahler: {min_strahler}')

    subcids = get_outlet_subcids_in_polygon(conn, polygon_geojson, min_strahler=min_strahler)
    LOGGER.info(f'Subcids: {subcids}')

    features = get_outlet_streamsegments_in_polygon(conn, polygon_geojson, min_strahler=min_strahler)
    LOGGER.info(f'Feature: {features}')


