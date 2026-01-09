import geojson
import logging
LOGGER = logging.getLogger(__name__)

try:
    # If the package is installed in local python PATH:
    import aqua90m.utils.exceptions as exc
    import aqua90m.utils.dataframe_utils as dataframe_utils
except ModuleNotFoundError as e1:
    try:
        # If we are using this from pygeoapi:
        import pygeoapi.process.aqua90m.utils.exceptions as exc
        import pygeoapi.process.aqua90m.utils.dataframe_utils as dataframe_utils
    except ModuleNotFoundError as e2:
        msg = 'Module not found: '+e1.name+' (imported in '+__name__+').' + \
              ' If this is being run from' + \
              ' command line, the aqua90m directory has to be added to ' + \
              ' PATH for python to find it.'
        print(msg)
        LOGGER.debug(msg)


def check_is_geojson(input_data):

    if not isinstance(input_data, dict):
        LOGGER.error('Input data cannot be GeoJSON: It is not a dict.')
        err_msg = "Input data is not GeoJSON."
        raise exc.UserInputException(err_msg)

    if not "type" in input_data:
        LOGGER.error('Input data cannot be GeoJSON: It does not have a type.')
        err_msg = "Input data is not GeoJSON (no 'type')."
        raise exc.UserInputException(err_msg)

    try:
        geojson.GeoJSON.to_instance(input_data)
        # this expects a string:
        #geojson_obj = geojson.loads(input_data)  # can also use geojson.dumps(data)
        # so we could do this:
        #geojson_obj = geojson.loads(json.dumps(input_data))
        # or this:
    except (ValueError, TypeError) as e:
        LOGGER.error(f'Input data cannot be GeoJSON: Cannot be loaded by library: {e}.')
        err_msg = f"Input data is not GeoJSON: {e}."
        raise exc.UserInputException(err_msg)


def check_is_geometry_collection_points(points_geojson):

    if not points_geojson['type'] == 'GeometryCollection':
        err_msg = 'GeoJSON is not a GeometryCollection.'
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)

    check_is_geojson(points_geojson)

    for point in points_geojson['geometries']:
        if not point['type'] == 'Point':
            err_msg = f'Geometries in GeometryCollection have to be points, not: {point["type"]}'
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)

    return True


def check_is_multipoints(points_geojson):

    if not points_geojson['type'] == 'MultiPoint':
        err_msg = 'GeoJSON is not a MultiPoint.'
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)

    check_is_geojson(points_geojson)

    return True


def check_feature_collection_property(feature_coll, mandatory_colname):
    if mandatory_colname is None:
        err_msg = f"Please provide the column name to check for each Feature in the FeatureCollection."
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)
    LOGGER.debug(f'Checking if "{mandatory_colname}" in FeatureCollection...')
    for feature in feature_coll['features']:
        LOGGER.debug(f'This feature: {feature}')
        LOGGER.debug(f"Properties of this feature: {feature['properties']}")
        if not mandatory_colname in feature['properties']:
            err_msg = f"Please provide '{mandatory_colname}' for each Feature in the FeatureCollection. Missing in: {feature}"
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)
    return True


def get_all_properties_per_id(feature_coll, colname_id):
    properties_by_id = {}
    for feature in feature_coll['features']:
        LOGGER.debug(f'This feature: {feature}')
        LOGGER.debug(f"Properties of this feature: {feature['properties']}")
        if not colname_id in feature['properties']:
            err_msg = f"Please provide '{colname_id}' for each Feature in the FeatureCollection. Missing in: {feature}"
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)
        feature_id = feature['properties'][colname_id]
        properties_by_id[feature_id] = feature['properties']
    return properties_by_id


def check_is_feature_collection_points(points_geojson):

    if not points_geojson['type'] == 'FeatureCollection':
        err_msg = 'GeoJSON is not a FeatureCollection.'
        LOGGER.error(err_msg)
        raise exc.UserInputException(err_msg)

    check_is_geojson(points_geojson)

    for feature in points_geojson['features']:
        if not feature['geometry']['type'] == 'Point':
            err_msg = f'Features in FeatureCollection have to be points, not: {feature["type"]}'
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)

    return True


def filter_geojson_by_condition(points_geojson, keep_attribute, condition_dict):
    # Filter by numeric condition
    LOGGER.debug(f'Filtering property "{keep_attribute}" (condition {condition_dict}).')

    # Iterate over all features:
    filtered_features = []
    for feature in points_geojson['features']:
        val = feature["properties"][keep_attribute]
        LOGGER.debug(f'Property "{keep_attribute}" has value {val}.')
        if dataframe_utils.matches_filter_condition(condition_dict, val):
            # keep!!
            # Collect results in list:
            filtered_features.append(feature)

    # Finished collecting the results, now make GeoJSON FeatureCollection:
    feature_coll = {
        "type": "FeatureCollection",
        "features": filtered_features
    }
    return feature_coll


def filter_geojson(points_geojson, keep_attribute, keep_values):
    LOGGER.debug(f'Filtering property "{keep_attribute}" (condition: contains any of these: {keep_values}).')

    # Iterate over all features:
    filtered_features = []
    for feature in points_geojson['features']:
        val = feature["properties"][keep_attribute]
        if val in keep_values:
            # keep!!
            # Collect results in list:
            filtered_features.append(feature)

    # Finished collecting the results, now make GeoJSON FeatureCollection:
    feature_coll = {
        "type": "FeatureCollection",
        "features": filtered_features
    }
    return feature_coll


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
