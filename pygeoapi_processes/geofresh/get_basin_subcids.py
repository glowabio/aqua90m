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
curl -X POST https://$PYSERVER/processes/get-basin-subcids/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": false,
    "comment": "close to bremerhaven",
    "strahler_min": 4,
    "add_segment_ids": true
    },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

# Request a simple GeometryCollection, based on a basin_id
# Output: LineStrings (GeometryCollection)
# Tested: 2026-01-02
curl -X POST https://$PYSERVER/processes/get-basin-subcids/execution \
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
curl -X POST "https://$PYSERVER/processes/get-basin-subcids/execution" \
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
curl -X POST "https://$PYSERVER/processes/get-basin-subcids/execution" \
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


class BasinSubcidsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        # Input can be a basin:
        basin_id = data.get('basin_id', None) # optional, need either basin_id OR lonlat/point OR subc_id
        # Or a point, from which we infer the basin:
        point = data.get('point', None) # optional, ...
        lon = data.get('lon', None)     # optional, ...
        lat = data.get('lat', None)     # optional, ...
        subc_id  = data.get('subc_id',  None) # optional, ...
        min_strahler = data.get('min_strahler', None)
        comment = data.get('comment') # optional

        # Check presence:
        utils.at_least_one_param({
            "basin_id": basin_id,
            "subc_id": subc_id,
            "point": point,
            "pair of coordinates (lon and lat)": (lon and lat)
        })

        # If GeoJSON point is given, get coordinates:
        if point is not None:
            lon, lat = point.get('coordinates') or point['geometry']['coordinates']

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

        ## Get all subc_ids
        LOGGER.debug(f'Now, getting subc_ids for basin_id: {basin_id}')
        all_subcids = basic_queries.get_all_subcids_from_basinid(
                conn, LOGGER, basin_id, reg_id, min_strahler=min_strahler)

        # Note: This is not GeoJSON (on purpose), as we did not look for geometry:
        output_json = {
            "reg_id": reg_id,
            "basin_id": basin_id,
            "num_subcatchments": len(all_subcids),
            "subc_ids": all_subcids
        }
        if min_strahler is not None:
            output_json["min_strahler"] = min_strahler

        ## Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        return self.return_results('basin_subcatchment_ids', requested_outputs, output_json=output_json, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-basin-subcids'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Input: basin_id, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_id": 1288419,
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 2: Input: basin_id, min_strahler=4, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_id": 1288419,
            "min_strahler": 4,
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 3: Input: subc_id, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_id": 506586041,
            "comment": "test3"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 4: Input: lon, lat, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 8.278198242187502,
            "lat": 53.54910661890981,
            "comment": "test3"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 5: Input: point, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs":
            {
            "point": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [8.278198242187502, 53.54910661890981]
                }
            },
            "geometry_only": True,
            "comment": "test5"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 6: Input: basin_id, min_strahler=6, which is not present in that basin...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_id": 1288419,
            "min_strahler": 6,
            "comment": "test6"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        print(f'TEST CASE 6: EXPECTED: {e.response.json()["description"]}')
