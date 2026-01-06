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
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

# Request a FeatureCollection, based on a basin_id:
# Output: LineStrings (FeatureCollection)
# Tested: 2026-01-02
curl -X POST https://$PYSERVER/processes/get-basin-streamsegments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": false,
    "comment": "close to bremerhaven",
    "strahler_min": 4,
    "add_segment_ids": true
    }
}'

# Request a simple GeometryCollection, based on a basin_id
# Output: LineStrings (GeometryCollection)
# Tested: 2026-01-02
curl -X POST https://$PYSERVER/processes/get-basin-streamsegments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": true,
    "comment": "close to bremerhaven"
    }
}'

# Request a simple GeometryCollection, based on a subc_id
# Output: LineStrings (GeometryCollection)
# Tested: 2026-01-02
curl -X POST "https://$PYSERVER/processes/get-basin-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_id": 506586041,
    "geometry_only": true,
    "comment": "close to bremerhaven"
    }
}'


# Request a simple GeometryCollection, based on lon+lat
# Output: LineStrings (GeometryCollection)
# Tested: 2026-01-02
curl -X POST "https://$PYSERVER/processes/get-basin-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 8.278198242187502,
    "lat": 53.54910661890981,
    "geometry_only": true
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class BasinStreamSegmentsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id  = data.get('subc_id',  None) # optional, need either lonlat OR subc_id
        basin_id = data.get('basin_id', None) # optional, need either lonlat OR subc_id
        strahler_min = data.get('strahler_min', 0)
        geometry_only = data.get('geometry_only', False)
        add_segment_ids = data.get('add_segment_ids', False)
        comment = data.get('comment') # optional

        # Check type:
        utils.is_bool_parameters(dict(
            geometry_only=geometry_only,
            add_segment_ids=add_segment_ids
        ))

        # Check presence:
        utils.at_least_one_param({
            "basin_id": basin_id,
            "subc_id": subc_id,
            "pair of coordinates (lon and lat)": (lon and lat)
        })

        ## Get reg_id, basin_id, subc_id - whatever is missing:
        if subc_id is not None:
            LOGGER.info(f'Retrieving basin_id for subc_id {subc_id}')
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        elif lon is not None and lat is not None:
            LOGGER.info(f'Retrieving basin_id for lon, lat: {lon}, {lat}')
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)
        elif basin_id is not None:
            reg_id = basic_queries.get_regid_from_basinid(conn, LOGGER, basin_id)

        ## Get GeoJSON geometry:
        LOGGER.debug(f'Now, getting stream segments for basin_id: {basin_id}')
        geojson_collection = None
        if geometry_only:
            geojson_collection = get_linestrings.get_streamsegment_linestrings_geometry_coll_by_basin(
                conn, basin_id, reg_id, strahler_min = strahler_min)
        else:
            geojson_collection = get_linestrings.get_streamsegment_linestrings_feature_coll_by_basin(
                conn, basin_id, reg_id, strahler_min = strahler_min)

            if add_segment_ids:
                segment_ids = []
                for item in geojson_collection['features']:
                    segment_ids.append(item["properties"]["subc_id"])
                geojson_collection['segment_ids'] = segment_ids

        ## Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        return self.return_results('stream_segments', requested_outputs, output_df=None, output_json=geojson_collection, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-basin-streamsegments'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Input: basin_id, output: FeatureCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_id": 1288419,
            "geometry_only": False,
            "strahler_min": 4,
            "add_segment_ids": True,
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 2: Input: basin_id, output: GeometryCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_id": 1288419,
            "geometry_only": True,
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 3: Input: subc_id, output: GeometryCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_id": 506586041,
            "geometry_only": True,
            "comment": "test3"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 4: Input: lon, lat, output: GeometryCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 8.278198242187502,
            "lat": 53.54910661890981,
            "geometry_only": True,
            "comment": "test3"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)
