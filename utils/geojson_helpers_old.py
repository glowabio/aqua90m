import logging
LOGGER = logging.getLogger(__name__)


def any_points_to_MultiPointGeometryCollection(LOGGER,
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
                    raise ValueError(err_msg)

            MultiPoint_geometrycoll = points_geojson

        elif points_geojson['type'] == 'FeatureCollection':

            for feature in points_geojson['features']:
                if not feature['geometry']['type'] == 'Point':
                    err_msg = 'Features in FeatureCollection have to be points, not: %s' % feature['type']
                    LOGGER.error(err_msg)
                    raise ValueError(err_msg)

                MultiPoint_geometrycoll['geometries'].append(feature['geometry'])

    return MultiPoint_geometrycoll



def any_points_to_MultiPointFeatureCollection(LOGGER,
    points_geojson = None,
    lonlatstring = None,
    csv = None):

    # We collect all points as GeoJSON GeometryCollection:
    MultiPoint_featurecoll = {
        'type': 'FeatureCollection',
        'features': []
    }

    # To check if we need to assign site_ids:
    # TODO: Just allow ALL or NONE. User can be expected to provide proper inputs.
    any_without_site_id = False
    all_without_site_id = True

    # Quick and dirty: Points are provided as pairs in string:
    if lonlatstring is not None:
        LOGGER.debug('Found lon,lat pairs: %s' % lonlatstring)

        lonlat = lonlatstring.split(';')
        num_pairs = 0
        for coordinate_pair in lonlat:
            split_parts = coordinate_pair.split(',')
            if len(split_parts) == 3:
                lon, lat, site_id = split_parts
                all_without_site_id = False
                # TODO: Do we need to check whether site_id is unique?
            elif len(split_parts) == 2:
                lon, lat = split_parts
                site_id = None
                any_without_site_id = True
                err_msg = 'No site_id in row: %s' % coordinate_pair
                LOGGER.warning(err_msg)
            else:
                err_msg = 'Unexpected number of values in row "%s"' % coordinate_pair

            MultiPoint_featurecoll['features'].append({
               "type": "Feature",
               "geometry": {
                  "type": "Point",
                   "coordinates": [lon, lat]
               },
               "properties": {
                   "site_id": site_id
               }
            })
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

            # GeometryCollections never have site_ids, as they don't have
            # "properties", so we set:
            any_without_site_id = True
            all_without_site_id = True

            for point in points_geojson['geometries']:

                if not point['type'] == 'Point':
                    err_msg = 'Geometries in GeometryCollection have to be points, not: %s' % feature['type']
                    LOGGER.error(err_msg)
                    raise ValueError(err_msg)

                feature = {
                   "type": "Feature",
                   "geometry": point,
                   "properties": {}
                }

                MultiPoint_featurecoll['features'].append(feature)

        elif points_geojson['type'] == 'FeatureCollection':

            MultiPoint_featurecoll = points_geojson

            # Verify whether features are valid, and check for site_ids:
            for feature in MultiPoint_featurecoll['features']:

                if not feature['geometry']['type'] == 'Point':
                    err_msg = 'Features in FeatureCollection have to be points, not: %s' % feature['type']
                    LOGGER.error(err_msg)
                    raise ValueError(err_msg)

                if 'site_id' in feature['properties']:
                    all_without_site_id = False
                    # TODO: Do we need to check whether site_id is unique?
                else:
                    any_without_site_id = True
                    err_msg = 'No site_id in feature: %s' % feature
                    LOGGER.warning(err_msg)

    # If no feature has a site id, just iterate over all of them:
    if any_without_site_id and all_without_site_id:
        site_id = 0
        for feature in MultiPoint_featurecoll['features']:
            site_id += 1
            feature['properties']['site_id'] = site_id

    # If some do have a site_id, we need to check which ones already
    # exist, to avoid collisions:
    if any_without_site_id and not all_without_site_id:
        existing_site_ids = set()
        for feature in MultiPoint_featurecoll['features']:
            if 'site_id' in feature['properties']:
                site_id = feature['properties']['site_id']

                # Remember site_id also as string and int (if it can
                # be transformed to int), to avoid having "1" and 1 at
                # the end...:
                existing_site_ids.add(site_id)
                existing_site_ids.add(str(site_id))
                try:
                    existing_site_ids.add(int(site_id))
                except (ValueError, TypeError):
                    pass

        tmp = ", ".join([str(elem) for elem in existing_site_ids])
        LOGGER.debug("XXX All these site_ids exist: %s" % tmp)

        # Now that we collected all existing ones, assign site_ids to
        # those who don't have one...
        final_set_site_ids = set() # only for logging
        site_id = 1
        for feature in MultiPoint_featurecoll['features']:


            # These two lines are only for logging:
            if 'site_id' in feature['properties'] and feature['properties']['site_id'] is not None:
                LOGGER.debug("HAS ID: Feature... %s" % feature)
                final_set_site_ids.add(feature['properties']['site_id'])

            if not 'site_id' in feature['properties'] or feature['properties']['site_id'] is None:
                LOGGER.debug("HAS NO ID: Feature... %s" % feature)

                # increment by 1 until we found one that is not there yet...
                while (True):
                #while (site_id in existing_site_ids):
                    if site_id in existing_site_ids:
                        site_id += 1
                    else:
                        break

                feature['properties']['site_id'] = site_id
                existing_site_ids.add(site_id)
                final_set_site_ids.add(site_id)
                LOGGER.debug('Added site_id=%s to feature...' % site_id)

        tmp = ", ".join([str(elem) for elem in final_set_site_ids])
        LOGGER.debug("XXX All these site_ids exist NOW: %s" % tmp)



    return MultiPoint_featurecoll


if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    LOGGER = logging.getLogger(__name__)

    # First, GeometryCollections:

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointGeometryCollection (using input feature coll)')
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
    res = any_points_to_MultiPointGeometryCollection(LOGGER, points_geojson = feature_coll)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointGeometryCollection (using lonlatstring)')
    lonlatstring = "10.698832912677716,53.51710727672125;12.80898022975407,52.42187129944509;11.915323076217902,52.730867141970464;16.651903948708565,48.27779486850176;19.201146608148463,47.12192880511424;24.432498016999062,61.215505889934434"
    res = any_points_to_MultiPointGeometryCollection(LOGGER, lonlatstring = lonlatstring)
    print('RESULT: %s' % res)

    # Now, FeatureCollections:
    # Input has NO site_ids:

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointFeatureCollection (using input feature coll with NO site_id)')
    res = any_points_to_MultiPointFeatureCollection(LOGGER, points_geojson = feature_coll)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointFeatureCollection (using lonlatstring with NO site_id)')
    res = any_points_to_MultiPointFeatureCollection(LOGGER, lonlatstring = lonlatstring)
    print('RESULT: %s' % res)

    # Now, FeatureCollections:
    # Input partially has site_ids:

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointFeatureCollection (using input feature coll with SOME site_id)')
    feature_coll3 = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"site_id": "a"},
                "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": "3"},
                "geometry": { "coordinates": [ 11.915323076217902, 52.730867141970464 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 16.651903948708565, 48.27779486850176 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": 5},
                "geometry": { "coordinates": [ 19.201146608148463, 47.12192880511424 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": { "coordinates": [ 24.432498016999062, 61.215505889934434 ], "type": "Point" }
            }
        ]
    }
    # Existing ids: "a", "3", 5
    # Will have to be: "a", 1, "3", 2, 5, 4
    res = any_points_to_MultiPointFeatureCollection(LOGGER, points_geojson = feature_coll3)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointFeatureCollection (using lonlatstring with SOME site_id)')
    lonlatstring3 = "10.698832912677716,53.51710727672125,a;12.80898022975407,52.42187129944509;11.915323076217902,52.730867141970464,3;16.651903948708565,48.27779486850176;19.201146608148463,47.12192880511424,5;24.432498016999062,61.215505889934434"
    # Existing ids: "a", "3", "5"
    # Will have to be: "a", 1, "3", 2, 5, 4
    res = any_points_to_MultiPointFeatureCollection(LOGGER, lonlatstring = lonlatstring3)
    print('RESULT: %s' % res)


    # Now, FeatureCollections:
    # Input HAS site_ids:

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointFeatureCollection (using input feature coll WITH site_id)')
    feature_coll2 = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"site_id": "a"},
                "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": "b"},
                "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": "3"},
                "geometry": { "coordinates": [ 11.915323076217902, 52.730867141970464 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": "4d"},
                "geometry": { "coordinates": [ 16.651903948708565, 48.27779486850176 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": "5e"},
                "geometry": { "coordinates": [ 19.201146608148463, 47.12192880511424 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": 6},
                "geometry": { "coordinates": [ 24.432498016999062, 61.215505889934434 ], "type": "Point" }
            }
        ]
    }
    res = any_points_to_MultiPointFeatureCollection(LOGGER, points_geojson = feature_coll2)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: any_points_to_MultiPointFeatureCollection (using lonlatstring WITH site_id)')
    lonlatstring2 = "10.698832912677716,53.51710727672125,1;12.80898022975407,52.42187129944509,2;11.915323076217902,52.730867141970464,3;16.651903948708565,48.27779486850176,4;19.201146608148463,47.12192880511424,5;24.432498016999062,61.215505889934434,6"
    res = any_points_to_MultiPointFeatureCollection(LOGGER, lonlatstring = lonlatstring2)
    print('RESULT: %s' % res)
