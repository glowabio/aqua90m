import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
import pandas as pd
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
## INPUT:  GeoJSON directly (Multipoint)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson": {
      "type": "MultiPoint",
      "coordinates": [
        [9.937520027160646, 54.69422745526058],
        [9.9217, 54.6917],
        [9.9312, 54.6933]
      ]
    },
    "comment": "located in schlei area"
  }
}'

## INPUT:  GeoJSON File (Multipoint)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometry_multipoint.json",
    "comment": "not sure where"
  }
}'

## INPUT:  GeoJSON File (Multipoint)
## OUTPUT: Plain JSON File
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometry_multipoint.json",
    "comment": "not sure where"
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'


## INPUT:  GeoJSON File (GeometryCollection)
## OUTPUT: Plain JSON File
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometrycollection_points_samebasin.json",
    "comment": "not sure where"
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'


## INPUT:  GeoJSON File (FeatureCollection)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points_samebasin.json",
    "comment": "not sure where"
  }
}'

## Fails because not in one basin:
## INPUT:  GeoJSON File (FeatureCollection)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
    "comment": "not sure where"
  }
}'

## INPUT:  subc_ids
## OUTPUT: Plain JSON directly
## Tested 2026-01-07
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "subc_ids": [506251712, 506252055, 506251712, 506251713],
        "comment": "not sure where"
  }
}'

## Not Implemented yet:
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "subc_ids": [506251712, 506252055],
        "subc_ids_end": [506251712, 506251713],
        "comment": "not sure where"
  }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class ShortestPathBetweenPointsGetterPlural(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        ####################
        ### User inputs: ###
        ####################

        # Singular case:
        # Two points:
        point_start = data.get('point_start', None)
        point_end = data.get('point_end', None)
        lon_start = data.get('lon_start', None)
        lat_start = data.get('lat_start', None)
        lon_end = data.get('lon_end', None)
        lat_end = data.get('lat_end', None)
        # Two subcatchments:
        subc_id_start = data.get('subc_id_start', None) # optional, need either lonlat OR subc_id
        subc_id_end = data.get('subc_id_end', None)     # optional, need either lonlat OR subc_id

        # Plural case:
        # GeoJSON (e.g. Multipoint, GeometryCollection of Points,
        # FeatureCollection of Points), posted directly:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)
        # Two separate sets of points:
        points_geojson_end = data.get('points_geojson_end', None)
        points_geojson_end_url = data.get('points_geojson_end_url', None)
        # Set of subcatchments:
        subc_ids = data.get('subc_ids', None)
        # Two separate sets of subcatchments:
        subc_ids_end = data.get('subc_ids_end', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', 'lon')
        colname_lat = data.get('colname_lat', 'lat')
        #colname_site_id = data.get('colname_site_id', None)
        # Output format (can be csv or json):
        result_format = data.get('result_format', 'json')
        # Comment:
        comment = data.get('comment') # optional

        ##############################
        ### Download if applicable ###
        ##############################

        ## Download GeoJSON if user provided URL:
        if points_geojson_url is not None:
            points_geojson = utils.download_geojson(points_geojson_url)
            LOGGER.debug(f'Downloaded GeoJSON: {points_geojson}')

        if points_geojson_end_url is not None:
            points_geojson_end = utils.download_geojson(points_geojson_end_url)
            LOGGER.debug(f'Downloaded GeoJSON: {points_geojson_end}')

        input_df = None
        if csv_url is not None:
            input_df = utils.access_csv_as_dataframe(csv_url)
            LOGGER.debug('Input CSV: Found {ncols} columns (names: {colnames})'.format(
                ncols=input_df.shape[1], colnames=input_df.columns))

            # Check if every row has id:
            #if not (colname_site_id in input_df.columns):
            #    err_msg = "Please add a column 'site_id' to your input dataframe."
            #    LOGGER.error(err_msg)
            #    raise ProcessorExecuteError(err_msg)

        #################################
        ### Validate input parameters ###
        #################################

        # Check result format
        if not result_format == 'json' and not result_format == 'csv':
            err_msg = f"Malformed parameter 'result_format': Format '{result_format}' not supported. Please specify 'csv' or 'json'."
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

        # If GeoJSON point is given, get coordinates:
        if point_start is not None:
            lon_start, lat_start = point_start.get('coordinates') or point_start['geometry']['coordinates']
        if point_end is not None:
            lon_end, lat_end = point_end.get('coordinates') or point_end['geometry']['coordinates']

        # Check GeoJSON validity
        if points_geojson is not None:
            geojson_helpers.check_is_geojson(points_geojson)
        if points_geojson_end is not None:
            geojson_helpers.check_is_geojson(points_geojson_end)

        ###########################
        ### Plural or singular? ###
        ###########################

        # Singular or plural case?
        singular = False
        plural_symmetric = False
        plural_asymmetric = False

        # Decide based on which inputs were provided:
        if not (lon_start is None
            and lat_start is None
            and lon_end is None
            and lat_end is None
            and subc_id_start is None
            and subc_id_end is None
            ):
            LOGGER.debug('Singular case...')
            singular = True
        elif not(points_geojson is None
             and points_geojson_end is None
             and subc_ids is None
             and subc_ids_end is None
             and input_df is None
            ):
            LOGGER.debug('Plural case...')
        else:
            err_msg = 'You must specify start and end point(s) as point(s) or subc_id(s).'
            raise ProcessorExecuteError(err_msg)

        if singular:
            return self.singular_case(conn, lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end, requested_outputs, comment, result_format)
        else:
            return self.plural_case(conn, points_geojson, points_geojson_end, subc_ids, subc_ids_end, input_df, colname_lon, colname_lat, requested_outputs, comment, result_format)


    def singular_case(self, conn, lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end, requested_outputs, comment, result_format):

        err_msg = "Currently not implemented: Singular case."
        raise NotImplementedError(err_msg)

        '''
        if not result_format == 'json':
            err_msg = f'Returning path between two point as {result_format} is not implemented.'
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

        # Check if either subc_id or both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon_start, lat_start, subc_id_start)
        utils.params_lonlat_or_subcid(lon_end,   lat_end,   subc_id_end)

        LOGGER.debug('START: Getting dijkstra shortest path between two points...')

        # Potential outputs
        json_result = None

        # First, get the ids required for querying the database efficiently:
        # Start point/start subcatchment:
        if lon_start is not None and lat_start is not None:
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, lon_start, lat_start)
        elif subc_id_start is not None:
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, subc_id = subc_id_start)

        # End point/end subcatchment:
        if lon_end is not None and lat_end is not None:
            subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                conn, lon_end, lat_end)
        elif subc_id_end is not None:
            subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                conn, subc_id = subc_id_end)

        # Check if same region and basin?
        # TODO: FUTURE MUSTIC: If start and end not in same basin, can we route via the sea?
        if not reg_id1 == reg_id2:
            err_msg = f'Start and end are in different regions ({reg_id1} and {reg_id2}) - this cannot work.'
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        if not basin_id1 == basin_id2:
            err_msg = f'Start and end are in different basins ({basin_id1} and {basin_id2}) - this cannot work.'
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        # Get distance - just a number:
        dist = distances.get_dijkstra_distance_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)
        json_result = {
            "distance": dist,
            "from": subc_id1,
            "to": subc_id2,
            "basin_id": basin_id1,
            "reg_id": reg_id1
        }

        return self.return_results('distances_matrix', requested_outputs, output_df=None, output_json=json_result, comment=comment)
        '''


    def plural_case(self, conn, points_geojson, points_geojson_end, subc_ids, subc_ids_end, input_df, colname_lon, colname_lat, requested_outputs, comment, result_format):

        # Symmetric or asymmetric matrix? I.e. are the start and end points
        # the same, or different sets?
        plural_symmetric = plural_asymmetric = False
        if not (
            subc_ids_end is None and
            points_geojson_end is None
        ):
            LOGGER.debug('Plural case, asymmetric matrix...')
            plural_asymmetric = True
        elif not(
            points_geojson is None and
            subc_ids is None and
            input_df is None
        ):
            LOGGER.debug('Plural case, symmetric matrix...')
            plural_symmetric = True

        # Get sets of input points
        if plural_symmetric:
            all_subc_ids_start, all_subc_ids_end, reg_id, basin_id = self.plural_symmetric(conn, points_geojson, subc_ids, input_df, colname_lon, colname_lat)
        elif plural_asymmetric:
            all_subc_ids_start, all_subc_ids_end, reg_id, basin_id = self.plural_asymmetric(conn, points_geojson, points_geojson_end, subc_ids, subc_ids_end)

        # Get shortest paths:
        output_df_or_json = routing.get_dijkstra_ids_many_to_many(
            conn, all_subc_ids_start, all_subc_ids_end, reg_id, basin_id, result_format)

        #####################
        ### Return result ###
        #####################

        output_df = output_json = None
        if isinstance(output_df_or_json, pd.DataFrame):
            output_df = output_df_or_json
        elif isinstance(output_df_or_json, dict):
            output_json = output_df_or_json

        return self.return_results('paths_matrix', requested_outputs, output_df=output_df, output_json=output_json, comment=comment)


    def plural_symmetric(self, conn, points_geojson, subc_ids, input_df, colname_lon, colname_lat):

        # Collect reg_id, basin_id, subc_id
        # Note: Without a site_id, the user cannot match them back to the input points!!!
        # TODO: Must match site_id of CSV/GeoJSON to subc_id!!!
        if points_geojson is not None:
            LOGGER.debug('START: Getting dijkstra shortest path between a number of points (start and end points are the same)...')
            temp_df = basic_queries.get_subcid_basinid_regid__geojson_to_dataframe(conn, points_geojson, colname_site_id=None)
            # TODO does this return NAs?
        elif subc_ids is not None:
            LOGGER.debug('START: Getting dijkstra shortest path between a number of subcatchments (start and end points are the same)...')
            all_subc_ids = set(subc_ids)
            temp_df = basic_queries.get_basinid_regid_from_subcid_plural(conn, subc_ids)
            # TODO does this return NAs?
        elif input_df is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of points (start and end points are the same)...')
            temp_df = basic_queries.get_subcid_basinid_regid__dataframe_to_dataframe(conn, input_df, colname_lon, colname_lat, colname_site_id=None)
            # TODO does this return NAs?

        # Retrieve subc_ids from the dataframe, and check if basins and regions match:
        all_subc_ids, reg_id, basin_id = self._get_ids_and_check(temp_df)

        # Return what's needed for routing:
        return all_subc_ids, all_subc_ids, reg_id, basin_id


    def plural_asymmetric(self, conn, points_geojson_start, points_geojson_end, subc_ids_start, subc_ids_end):
        LOGGER.debug('START: Getting dijkstra shortest distance between a number of subcatchments (start and end points are different)...')

        # Collect reg_id, basin_id, subc_id of set of start subcatchments:
        if points_geojson_start is not None:
            temp_df_start = basic_queries.get_subcid_basinid_regid__geojson_to_dataframe(conn, points_geojson_start, colname_site_id=None)
            # TODO does this return NAs?
        elif subc_ids_start is not None and subc_ids_end is not None:
            all_subc_ids_start = set(subc_ids_start)
            temp_df_start = basic_queries.get_basinid_regid_from_subcid_plural(conn, all_subc_ids_start)
            # TODO does this return NAs?

        # Collect reg_id, basin_id, subc_id of set of end subcatchments:
        if points_geojson_end is not None:
            temp_df_end   = basic_queries.get_subcid_basinid_regid__geojson_to_dataframe(conn, points_geojson_end, colname_site_id=None)
            # TODO does this return NAs?
        elif subc_ids_start is not None and subc_ids_end is not None:
            all_subc_ids_end = set(subc_ids_end)
            temp_df_end = basic_queries.get_basinid_regid_from_subcid_plural(conn, all_subc_ids_end)
            # TODO does this return NAs?

        # Retrieve subc_ids from the dataframe, and check if basins and regions match:
        all_subc_ids_start, reg_id1, basin_id1 = self._get_ids_and_check(temp_df_start)
        all_subc_ids_end,   reg_id2, basin_id2 = self._get_ids_and_check(temp_df_end)

        # We checked for same region and basin inside the start and end sets,
        # but not between the sets. It could be that all starts points are in
        # one region and all end points in another (unlikely but not impossible):
        if not (reg_id1 == reg_id2):
            err_msg = (
                f'The input points are in different regions (start: {reg_id1},'
                f' end: {reg_id2}) - this cannot work.'
            )
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        if not (basin_id1 == basin_id2):
            err_msg = (
                f'The input points are in different basins (start: {basin_id1},'
                f' end: {basin_id2}) - this cannot work.'
            )
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        # Return what's needed for routing:
        return all_subc_ids_start, all_subc_ids_end, reg_id1, basin_id1


    def _get_ids_and_check(self, temp_df):
        # Check if all rows of the dataframe have same basin and region,
        # and return the set of subc_id values.

        # Check if same region?
        if temp_df['reg_id'].nunique(dropna=False) == 1:
            reg_id = temp_df['reg_id'].iloc[0]
        else:
            all_reg_ids = temp_df['reg_id'].unique()
            err_msg = f'The input points are in different regions ({list(all_reg_ids)}) - this cannot work.'
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        # Check if same basin?
        # TODO: FUTURE MUSIC: If start and end not in same basin, can we route via the sea?
        if temp_df['basin_id'].nunique(dropna=False) == 1:
            basin_id = temp_df['basin_id'].iloc[0]
        else:
            all_basin_ids = temp_df['basin_id'].unique()
            err_msg = f'The input points are in different basins ({list(all_basin_ids)}) - this cannot work.'
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
    process_id = 'get-shortest-path-between-points-plural'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson

    #######################################
    ### Simple:                         ###
    ### Request path between two points ###
    #######################################

    ############################################
    ### Matrix:                              ###
    ### Request distance between many points ###
    ############################################

    print('TEST CASE 1: Input GeoJSON directly (Multipoint), output plain JSON directly...', end="", flush=True)  # no newline
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
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 2: Input GeoJSON File (Multipoint), output plain JSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometry_multipoint.json",
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 3: Input GeoJSON File (Multipoint), output plain JSON file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometry_multipoint.json",
            "comment": "test3"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 4: Input GeoJSON File (GeometryCollection), output plain JSON file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometrycollection_points_samebasin.json",
            "comment": "test4"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 5: Input GeoJSON File (FeatureCollection), output plain JSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points_samebasin.json",
            "comment": "test5"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 6: Will fail because not in one basin...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
            "comment": "test6"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        print(f'TEST CASE 6: EXPECTED: {e.response.json()["description"]}')


    print('TEST CASE 7: Matrix: Request paths between two sets of points, based on subc_ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_ids": [506251712, 506252055, 506251712, 506251713],
            "result_format": "json",
            "comment": "test7"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 8: Matrix as csv: Request paths between two sets of points, based on subc_ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_ids": [506251712, 506252055, 506251712, 506251713],
            "result_format": "csv",
            "comment": "test8"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 9: Matrix: Request paths between two sets of points, based on subc_ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_ids": [506251712, 506252055],
            "subc_ids_end": [506251712, 506251713],
            "comment": "test9"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 10: Matrix: Request paths between two sets of points, based on GeoJSON...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "MultiPoint",
                "coordinates": [
                    [9.9217, 54.6917],
                    [9.9312, 54.6933]
                ]
            },
            "points_geojson_end": {
                "type": "MultiPoint",
                "coordinates": [
                    [9.937520027160646, 54.69422745526058],
                    [9.9217478273, 54.69173489023]
                ]
            },
            "comment": "test10"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    print('TEST CASE 11: Input GeoJSON File (FeatureCollection), output plain JSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lon": "longitude",
            "colname_lat": "latitude",
            "result_format": "json",
            "comment": "test11"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    print('TEST CASE 12: Input GeoJSON File (FeatureCollection), output plain JSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "csv_url_end": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lon": "longitude",
            "colname_lat": "latitude",
            "result_format": "json",
            "comment": "test 12"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

