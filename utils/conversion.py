import logging
LOGGER = logging.getLogger(__name__)


def dataframe_to_geojson_points(input_df, colname_lon, colname_lat):
    LOGGER.debug(f'Input data frame has {input_df.shape[1]} columns: {input_df.columns}.')

    features = []

    # Retrieve using column index, not colname - this is faster:
    colidx_lon = input_df.columns.get_loc(colname_lon)
    colidx_lat = input_df.columns.get_loc(colname_lat)
    for row in input_df.itertuples(index=False):
        lon = row[colidx_lon]
        lat = row[colidx_lat]
        feature = {
          "type": "Feature",
          "geometry": {
            "type": "Point",
            "coordinates": [lon, lat]
          }
        }
        properties = {}
        for colname in input_df.columns:
            val = getattr(row, colname)
            properties[colname] = val
        feature["properties"] = properties
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }

def geojson_points_to_dataframe(points_geojson, colname_lon="lon", colname_lat="lat"):
    # Input must be a FeatureCollection of Point features!

    # The future dataframe needs a list for each future column. The columns
    # lon and lat are not properties of the Features, but parts of the geometries,
    # so we predefine them here:
    everything = {
        colname_lon: [],
        colname_lat: []
    }

    # Find all properties:
    property_names = set()
    for feature in points_geojson["features"]:
        for propname in feature["properties"].keys():
            property_names.add(propname)
            # The future dataframe needs a list for each future column:
            everything[propname] = []

    # Iterate over all features, copy the property values into the dict
    # that will become the data frame:
    for feature in points_geojson["features"]:
        lon, lat = feature["geometry"]["coordinates"]
        everything[colname_lon].append(lon)
        everything[colname_lat].append(lon)
        for propname in property_names:
            propval = feature["properties"][propname]
            everything[propname].append(propval)

    # Finished collecting the values, now make pandas dataframe:
    dataframe = pd.DataFrame(everything)
    return dataframe
