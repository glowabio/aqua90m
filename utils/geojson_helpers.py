import logging
LOGGER = logging.getLogger(__name__)

try:
    # If the package is installed in local python PATH:
    import aqua90m.utils.exceptions as exc
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.exceptions as exc
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)



def check_is_geometry_collection_points(points_geojson):

    if not 'geometries' in points_geojson:
        err_msg = 'GeometryCollection has to contain "geometries".'
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)

    for point in points_geojson['geometries']:
        if not point['type'] == 'Point':
            err_msg = 'Geometries in GeometryCollection have to be points, not: %s' % point['type']
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)

    return True

def check_feature_collection_property(feature_coll, mandatory_colname):
    for feature in feature_coll['features']:
        if not mandatory_colname in feature['properties']:
            err_msg = f"Please provide '{mandatory_colname}' for each Feature in the FeatureCollection. Missing in: {feature}"
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)

    return True


def check_is_feature_collection_points(points_geojson):

    if not 'features' in points_geojson:
        err_msg = 'FeatureCollection has to contain "features".'
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)

    for feature in points_geojson['features']:
        if not feature['geometry']['type'] == 'Point':
            err_msg = 'Features in FeatureCollection have to be points, not: %s' % feature['type']
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)

    return True



if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    LOGGER = logging.getLogger(__name__)

    # First, FeatureCollections:

    feature_coll = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"site_id": "1"},
                "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": "2"},
                "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"site_id": "3"},
                "geometry": { "coordinates": [ 11.915323076217902, 52.730867141970464 ], "type": "Point" }
            }
        ]
    }
    geometry_coll = {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "Point",
                "coordinates": [ 10.041155219078064, 53.07006147583069 ]
            },
            {
                "type": "Point",
                "coordinates": [ 10.042726993560791, 53.06911450500803 ]
            },
            {
                "type": "Point",
                "coordinates": [ 10.039894580841064, 53.06869677412868 ]
            }
        ]
    }

    print('\nSTART RUNNING FUNCTION: check_is_feature_collection_points (using input feature coll)')
    res = check_is_feature_collection_points(feature_coll)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: check_is_geometry_collection_points (using input geometry coll)')
    res = check_is_geometry_collection_points(geometry_coll)
    print('RESULT: %s' % res)

    print('\nSTART RUNNING FUNCTION: check_feature_collection_property (using input feature coll)')
    res = check_feature_collection_property(feature_coll, "site_id")
    print('RESULT: %s' % res)

    ## Now when things go wrong:
    feature_coll_missing = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"sote_id": "1"},
                "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"sote_id": "1"},
                "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
            },
            {
                "type": "Feature",
                "properties": {"sote_id": "1"},
                "geometry": { "coordinates": [ 11.915323076217902, 52.730867141970464 ], "type": "Point" }
            }
        ]
    }

    print('\nSTART RUNNING FUNCTION: check_feature_collection_property (using input feature coll)')
    try:
        res = check_feature_collection_property(feature_coll_missing, "site_id")
        raise RuntimeError("This should not have functioned!")
    except exc.UserInputException as e:
        print('RESULT: %s' % e)
