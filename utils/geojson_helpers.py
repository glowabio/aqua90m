import logging

def any_points_to_MultiPointCollection(LOGGER,
    points_geojson = None,
    lonlatstring = None,
    csv = None):

    # We collect all points as GeoJSON GeometryCollection:
    MultiPoint_geometrycoll = {
        'type': 'GeometryCollection',
        'geometries': []
    }

    # Quick and dirty: Points are provided as pairs in string:
    if lonlatstring is not None:
        LOGGER.debug('Found lon,lat pairs: %s' % lonlatstring)

        lonlat = lonlatstring.split(';')
        num_pairs = 0
        for coordinate_pair in lonlat:
            lon, lat = coordinate_pair.split(',')
            MultiPoint_geometrycoll['geometries'].append(
                {'type':'Point', 'coordinates':[lon, lat]})
            #LOGGER.debug('Added: %s' % {'type':'Point', 'coordinates':[lon, lat]})
            num_pairs += 1

        LOGGER.info('Found %s lon,lat pairs...' % num_pairs)

    # Points are provided in CSV columns:
    elif csv is not None:
        LOGGER.error('Not implemented!!')
        raise ValueError('Not implemented!!')

        '''
        # In which format do we get it? Just as string I guess...
        # Add the received input to temp dir:
        output_temp_dir = WIP TODO
        input_temp_path = output_temp_dir+os.sep+'someInputsWIP.csv'
        with open(input_temp_path, 'w') as inputfile:
            inputfile.write(csv)
            # TODO Clean up csv files!
        '''

    elif points_geojson is not None:

        if points_geojson['type'] == 'GeometryCollection':
            
            for point in points_geojson['geometries']:
                if not point['type'] == 'Point':
                    err_msg = 'Geometries in GeometryCollection have to be points, not: %s' % feature['type']
                    LOGGER.error(err_msg)
                    raise Value(err_msg)
            
            MultiPoint_geometrycoll = points_geojson

        elif points_geojson['type'] == 'FeatureCollection':

            for feature in points_geojson['features']:
                if not feature['geometry']['type'] == 'Point':
                    err_msg = 'Features in FeatureCollection have to be points, not: %s' % feature['type']
                    LOGGER.error(err_msg)
                    raise Value(err_msg)

                MultiPoint_geometrycoll['geometries'].append(feature['geometry'])

    return MultiPoint_geometrycoll


if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    LOGGER = logging.getLogger(__name__)

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointCollection (using ...)')
    feature_coll = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 11.915323076217902, 52.730867141970464 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 16.651903948708565, 48.27779486850176 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 19.201146608148463, 47.12192880511424 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 24.432498016999062, 61.215505889934434 ], "type": "Point" }
            }
        ]
    }
    res = any_points_to_MultiPointCollection(LOGGER, points_geojson = feature_coll)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointCollection (using ...)')
    lonlatstring = "10.698832912677716,53.51710727672125;12.80898022975407,52.42187129944509;11.915323076217902,52.730867141970464;16.651903948708565,48.27779486850176;19.201146608148463,47.12192880511424;24.432498016999062,61.215505889934434"
    res = any_points_to_MultiPointCollection(LOGGER, lonlatstring = lonlatstring)
    print('RESULT: %s' % res)
