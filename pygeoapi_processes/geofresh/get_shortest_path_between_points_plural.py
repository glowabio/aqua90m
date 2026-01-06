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
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
## INPUT:  GeoJSON directly (Multipoint)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points": {
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
## Tested 2026-01-02 WIP
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
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

        # User inputs
        # GeoJSON, posted directly
        points = data.get('points', None)
        # GeoJSON, to be downloaded via URL:
        points_geojson_url = data.get('points_geojson_url', None)
        #lon_start = data.get('lon_start', None)
        #lat_start = data.get('lat_start', None)
        #lon_end = data.get('lon_end', None)
        #lat_end = data.get('lat_end', None)
        #subc_id_start = data.get('subc_id_start', None) # optional, need either lonlat OR subc_id
        #subc_id_end = data.get('subc_id_end', None)     # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        #add_segment_ids = data.get('add_segment_ids', True) # TODO Implement param 'add_segment_ids'
        #geometry_only = data.get('geometry_only', False) # TODO Implement param 'geometry_only'

        ## Check if boolean:
        #utils.is_bool_parameters(dict(
        #    add_segment_ids=add_segment_ids,
        #    geometry_only=geometry_only))

        ## Download GeoJSON if user provided URL:
        if points_geojson_url is not None:
            points = utils.download_geojson(points_geojson_url)
            LOGGER.debug(f'Downloaded GeoJSON: {points}')

        # Overall goal: Get the dijkstra shortest path (as linestrings)!
        #LOGGER.info('START: Getting dijkstra shortest path for lon %s, lat %s (or subc_id %s) to lon %s, lat %s (or subc_id %s)' % (
        #    lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end))

        # Get reg_id, basin_id, subc_id
        all_subc_ids = []
        all_reg_ids = []
        all_basin_ids = []
        # TODO: Looping over features/points is not good. Also, we loop twice (for Geometry/Feature).
        if 'coordinates' in points:
            for lon, lat in points['coordinates']:
                # TODO: Loop may not be most efficient!
                LOGGER.debug('Now getting subc_id, basin_id, reg_id for lon %s, lat %s' % (lon, lat))
                subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon, lat)
                all_subc_ids.append(subc_id)
                all_reg_ids.append(reg_id)
                all_basin_ids.append(basin_id)
        elif 'geometries' in points:
            for geom in points['geometries']:
                lon, lat = geom['coordinates']
                # TODO: Loop may not be most efficient!
                LOGGER.debug('Now getting subc_id, basin_id, reg_id for lon %s, lat %s' % (lon, lat))
                subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon, lat)
                all_subc_ids.append(subc_id)
                all_reg_ids.append(reg_id)
                all_basin_ids.append(basin_id)
        elif 'features' in points:
            for feature in points['features']:
                lon, lat = feature['geometry']['coordinates']
                # TODO: Loop may not be most efficient!
                LOGGER.debug('Now getting subc_id, basin_id, reg_id for lon %s, lat %s' % (lon, lat))
                subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon, lat)
                all_subc_ids.append(subc_id)
                all_reg_ids.append(reg_id)
                all_basin_ids.append(basin_id)

        # Check if same region and basin?
        # TODO: Can we route via the sea then??
        if len(set(all_reg_ids)) == 1:
            reg_id = all_reg_ids[0]
        else:
            err_msg = 'The input points are in different regions (%s) - this cannot work.' % set(all_reg_ids)
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        if len(set(all_basin_ids)) == 1:
            basin_id = all_basin_ids[0]
        else:
            err_msg = 'The input points are in different basins (%s) - this cannot work.' % set(all_basin_ids)
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        #if not reg_id1 == reg_id2:
        #    err_msg = 'Start and end are in different regions (%s and %s) - this cannot work.' % (reg_id1, reg_id2)
        #    LOGGER.warning(err_msg)
        #    raise ProcessorExecuteError(user_msg=err_msg)

        #if not basin_id1 == basin_id2:
        #    err_msg = 'Start and end are in different basins (%s and %s) - this cannot work.' % (basin_id1, basin_id2)
        #    LOGGER.warning(err_msg)
        #    raise ProcessorExecuteError(user_msg=err_msg)

        # Get subc_ids of the whole connection...
        #LOGGER.debug('Getting network connection for subc_id: start = %s, end = %s' % (subc_id1, subc_id2))
        #segment_ids = routing.get_dijkstra_ids_one_to_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)
        some_json_result = routing.get_dijkstra_ids_many_to_many(conn, all_subc_ids, reg_id, basin_id)

        if comment is not None:
            some_json_result['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        return self.return_results('paths_matrix', requested_outputs, output_df=None, output_json=some_json_result, comment=comment)

        # Get geometry only:
        #if geometry_only:
        #    geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(
        #        conn, segment_ids, basin_id1, reg_id1)

        #    if comment is not None:
        #        geometry_coll['comment'] = comment

        #    if self.return_hyperlink('connecting_path', requested_outputs):
        #        return 'application/json', self.store_to_json_file('connecting_path', geometry_coll)
        #    else:
        #        return 'application/json', geometry_coll


        # Get FeatureCollection
        #if not geometry_only:

        #    feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(
        #        conn, segment_ids, basin_id1, reg_id1, add_subc_ids = add_segment_ids)

        #    # Add some info to the FeatureCollection:
        #    feature_coll["description"] = "Connecting path between %s and %s" % (subc_id1, subc_id2)
        #    feature_coll["start_subc_id"] = subc_id1 # TODO how to name the start point of routing?
        #    feature_coll["target_subc_id"] = subc_id2 # TODO how to name the end point of routing?
        #    # TODO: Should we include the requested lon and lat? Maybe as a point?

        #    if comment is not None:
        #        feature_coll['comment'] = comment

        #    if self.return_hyperlink('connecting_path', requested_outputs):
        #        return 'application/json', self.store_to_json_file('connecting_path', feature_coll)
        #    else:
        #        return 'application/json', feature_coll



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


    print('TEST CASE 1: Input GeoJSON directly (Multipoint), output plain JSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points": {
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
