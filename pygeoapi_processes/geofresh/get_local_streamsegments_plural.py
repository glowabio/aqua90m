import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request Geometries (LineString):
# Tested: 2026-01-08
curl -X POST https://${PYSERVER}/processes/get-local-streamsegments-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
    "colname_lon": "longitude",
    "colname_lat": "latitude",
    "colname_site_id": "site_id",
    "geometry_only": true,
    "result_format": "json",
    "comment": "schlei-near-rabenholz"
  }
}'

# Request Features (LineString):
# Tested: 2026-01-08
curl -X POST https://${PYSERVER}/processes/get-local-streamsegments-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
    "colname_lon": "longitude",
    "colname_lat": "latitude",
    "colname_site_id": "site_id",
    "geometry_only": false,
    "result_format": "json",
    "comment": "schlei-near-rabenholz"
  }
}'
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalStreamSegmentsGetterPlural(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        ##############
        ### inputs ###
        ##############

        # Two options
        # Input:  GeoJSON with points
        # Input:  CSV with lon, lat (or with subc_id)

        # Outputs:
        # Output: GeoJSON with linestrings
        # Output: CSV ... HOW??? TODO ask Afroditi

        # User inputs
        #return_csv  = data.get('return_csv',  False)
        #return_json = data.get('return_json', False)
        # Output format (can be csv or json):
        result_format = data.get('result_format', 'json')

        # GeoJSON:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)

        # CSV (to be downloaded via URL):
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', 'lon')
        colname_lat = data.get('colname_lat', 'lat')
        colname_site_id = data.get('colname_site_id', None)
        # Other:
        comment = data.get('comment', None)
        geometry_only = data.get('geometry_only', False)


        ###################################
        ### Catch Not Implemented cases ###
        ###################################

        if result_format == 'csv':
            err_msg = (
                'Cannot return stream segments as CSV! (Let us know if you'
                ' would like this functionality, and how exactly).'
            )
            LOGGER.error(err_msg)
            raise NotImplementedError(err_msg)
        # Note: Creating a dataframe is not implemented yet.
        # If you implement this, check e.g. get_shortest_path_to_outlet_plural for a template...

        ##############################
        ### Download if applicable ###
        ##############################

        ## Download GeoJSON if user provided URL:
        if points_geojson_url is not None:
<<<<<<< HEAD
            points = utils.download_geojson(points_geojson_url)
            LOGGER.debug(f'Downloaded GeoJSON: {points}')
=======
            points_geojson = utils.download_geojson(points_geojson_url)
            LOGGER.debug(f'Downloaded GeoJSON: {points_geojson}')
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)

        # Download CSV:
        input_df = None
        if csv_url is not None:
            LOGGER.debug(f'Accessing input CSV from: {csv_url}')
            input_df = utils.access_csv_as_dataframe(csv_url)
            LOGGER.debug('Accessing input CSV... DONE. Found {ncols} columns (names: {colnames})'.format(
                ncols=input_df.shape[1], colnames=input_df.columns))

        #################################
        ### Validate input parameters ###
        #################################

        # Check if boolean:
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Check parameters:
        if csv_url is not None:
            if colname_site_id is None:
                err_msg = "If you provide a CSV file, you must provide colname_site_id!"
                LOGGER.error(err_msg)
                raise ProcessorExecuteError(err_msg)

<<<<<<< HEAD
=======
        # Validate GeoJSON:
        if points_geojson is not None:
            LOGGER.debug('POINTS GEOJSON: %s' % points_geojson)
            LOGGER.debug(f'POINTS GEOJSON: {type(points_geojson)}')
            geojson_helpers.check_is_geojson(points_geojson)

>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)
        ##################
        ### Actual ... ###
        ##################

        ## Potential outputs:
        output_json = None
        output_df = None
<<<<<<< HEAD
        properties_by_id = {}
=======

        # Needed during the process
        properties_by_id = {}
        all_subc_ids = reg_id = basin_id = None
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)

        ## Handle GeoJSON case:
        if points_geojson is not None:
            # TODO Should we check if we have subc_ids already?

            # If a FeatureCollections is passed, check whether the property
            # "site_id" (or similar) is present in every feature:
            if points_geojson['type'] == 'FeatureCollection':
                properties_by_siteid = geojson_helpers.get_all_properties_per_id(points_geojson, colname_site_id)

<<<<<<< HEAD
=======
            LOGGER.debug('Querying subc_id etc. for each point in input GeoJSON...')
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)
            #points_geojson = get_subcid_basinid_regid_for_all_2json(conn, LOGGER, points_geojson_with_siteid, colname_site_id)
            temp_df = basic_queries.get_subcid_basinid_regid_for_geojson(conn, 'localsegments', points_geojson, colname_site_id=colname_site_id)
            all_subc_ids, reg_id, basin_id = self._get_ids_and_check(temp_df)

        ## Handle CSV case:
        elif input_df is not None:

            ## Now, for each row, get the ids (unless already present)!
            # TODO: We need to match subc_ids with site_ids!!
            # From any input, we need to construct a data structure with site_id and subc_id... Feature Collection...
            # Which will be enriched with stream segment then...
            if ('subc_id' in input_df.columns and
                'reg_id' in input_df.columns and
                'reg_id' in input_df.columns):
                LOGGER.debug('Input dataframe already contains subc_id for each point, using that...')
            else:
<<<<<<< HEAD
                LOGGER.debug('Querying subc_id etc. for each point...')
=======
                LOGGER.debug('Querying subc_id etc. for each point in input dataframe...')
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)
                temp_df = basic_queries.get_subcid_basinid_regid_for_dataframe(conn, 'localsegments', input_df, colname_lon, colname_lat, colname_site_id)
            all_subc_ids, reg_id, basin_id = self._get_ids_and_check(temp_df)

        ## Next, for all subc_ids, get the stream segments!
        if result_format == 'csv':
            pass
        elif result_format == 'json':
            LOGGER.debug(f'Retrieve LineStrings for {len(all_subc_ids)} subc_ids.')
            # Make Collections with 1 LineString per subc_id, not 1 LineString per site_id, 
            # so there may be less LineStrings than original input points! TODO: Ok?
            if geometry_only:
                output_json = get_linestrings.get_streamsegment_linestrings_geometry_coll(
                    conn,
                    all_subc_ids,
                    basin_id,
                    reg_id)
                LOGGER.debug(f'This is the finished GeometryCollection: {output_json}')
            else:
                # We need to match subc_ids with site_ids!
                # Currently: We add all site ids to each feature...
                # Alternative: Make one feature per site_id, and add all properties...
                output_json = get_linestrings.get_streamsegment_linestrings_feature_coll(
                    conn,
                    all_subc_ids,
                    basin_id,
                    reg_id)
<<<<<<< HEAD
                LOGGER.debug(f'Add property {colname_site_id} to Features for {len(all_subc_ids)} subc_ids.')
                for feature in output_json['features']:
                    subc_id = feature['properties']['subc_id']
                    # Look up site id in temp_df, which connects "site_id" and "subc_id":
                    #site_id = temp_df.loc[df['subc_id'] == subc_id, 'site_id'].iloc[0]
                    site_ids = temp_df.loc[temp_df['subc_id'] == subc_id, 'site_id'].tolist()
                    # Add to current feature's properties:
                    feature['properties'][colname_site_id] = site_ids
                    # Add more props from original FeatureCollection:
                    # Note: This would only work if we returned one Feature per site_id,
                    # not one Feature per subc_id!
                    #try:
                    #    feature['properties'].update(properties_by_siteid[site_id])
                    #except KeyError as e:
                    #    pass
=======

                # Adding properties to features, if applicable...
                if colname_site_id is not None:
                    LOGGER.debug(f'Add property {colname_site_id} to Features for {len(all_subc_ids)} subc_ids.')
                    for feature in output_json['features']:
                        subc_id = feature['properties']['subc_id']
                        # Look up site id in temp_df, which connects "site_id" and "subc_id":
                        #site_id = temp_df.loc[df['subc_id'] == subc_id, 'site_id'].iloc[0]
                        site_ids = temp_df.loc[temp_df['subc_id'] == subc_id, 'site_id'].tolist()
                        # Add to current feature's properties:
                        feature['properties'][colname_site_id] = site_ids
                        # Add more props from original FeatureCollection:
                        # Note: This would only work if we returned one Feature per site_id,
                        # not one Feature per subc_id!
                        #try:
                        #    feature['properties'].update(properties_by_siteid[site_id])
                        #except KeyError as e:
                        #    pass
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)
                LOGGER.debug(f'This is the finished FeatureCollection: {output_json}')

            return self.return_results('stream_segments', requested_outputs, output_json=output_json, comment=comment)


    def _get_ids_and_check(self, temp_df):
        # Check if all rows of the dataframe have same basin and region,
        # and return the set of subc_id values.

        # Check if same region?
        if temp_df['reg_id'].nunique(dropna=False) == 1:
            reg_id = temp_df['reg_id'].iloc[0]
        else:
            all_reg_ids = temp_df['reg_id'].unique()
            err_msg = f'The input points are in different regions ({all_reg_ids}) - this cannot work.'
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        # Check if same basin?
        if temp_df['basin_id'].nunique(dropna=False) == 1:
            basin_id = temp_df['basin_id'].iloc[0]
        else:
            all_basin_ids = temp_df['basin_id'].unique()
            err_msg = f'The input points are in different basins ({all_basin_ids}) - this cannot work.'
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        # Get all unique subc_ids:
        all_subc_ids = temp_df['subc_id'].unique()

        # Return what's needed for routing:
        return all_subc_ids, reg_id, basin_id


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-local-streamsegments-plural'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson

    print('TEST CASE 1: Input GeoJSON directly (Multipoint), output plain JSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
<<<<<<< HEAD
            "points": {
=======
            "points_geojson": {
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)
                "type": "MultiPoint",
                "coordinates": [
                    [9.937520027160646, 54.69422745526058],
                    [9.9217, 54.6917],
                    [9.9312, 54.6933]
                ]
            },
<<<<<<< HEAD
=======
            "result_format": "json",
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
<<<<<<< HEAD
    sanity_checks_basic(resp)
=======
    sanity_checks_geojson(resp)


    print('TEST CASE 2: Same, but asking for a file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "MultiPoint",
                "coordinates": [
                    [9.937520027160646, 54.69422745526058],
                    [9.9217, 54.6917],
                    [9.9312, 54.6933]
                ]
            },
            "result_format": "json",
            "comment": "test2"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 3: Will fail, as we request csv format...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "MultiPoint",
                "coordinates": [
                    [9.937520027160646, 54.69422745526058],
                    [9.9217, 54.6917],
                    [9.9312, 54.6933]
                ]
            },
            "result_format": "csv",
            "comment": "test3"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        print(f'TEST CASE 3: EXPECTED: {e.response.json()["description"]}')
>>>>>>> e87dec0 (New process: get_local_streamsegments_plural.)
