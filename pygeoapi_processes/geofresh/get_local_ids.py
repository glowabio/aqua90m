import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
import pygeoapi.process.aqua90m.utils.exceptions as exc
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

'''

# Request all ids
# Tested: 2025-01-05
curl -X POST https://${PYSERVER}/processes/get-local-ids/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": ["subc_id", "basin_id", "reg_id"],
    "comment": "schlei-near-rabenholz"
  }
}'

# Request all ids, but as file
# Tested: 2025-01-05
curl -X POST https://${PYSERVER}/processes/get-local-ids/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": ["subc_id", "basin_id", "reg_id"],
    "comment": "schlei-near-rabenholz"
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

# Request only reg_id
# Tested: 2025-01-05
curl -X POST https://${PYSERVER}/processes/get-local-ids/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": "reg_id",
    "comment": "schlei-near-rabenholz"
  }
}'

# Request only reg_id, better:
# Tested: 2025-01-05
curl -X POST https://${PYSERVER}/processes/get-local-ids/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": ["reg_id"],
    "comment": "schlei-near-rabenholz"
  }
}'

# Request only basin_id
# Tested: 2025-01-05
curl -X POST https://${PYSERVER}/processes/get-local-ids/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": "basin_id",
    "comment": "schlei-near-rabenholz"
  }
}'

# Special case: Request all ids, when we know the subc_id!
# Tested: 2025-01-05
curl -X POST https://${PYSERVER}/processes/get-local-ids/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_id": 506250459,
    "which_ids": ["subc_id", "basin_id", "reg_id"],
    "comment": "schlei-near-rabenholz"
  }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalIdGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        point = data.get('point', None)
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        site_id = data.get('site_id') # optional
        which_ids = data.get('which_ids', ['subc_id', 'basin_id', 'reg_id'])
        result_format = data.get('result_format', None)

        #######################
        ### Validate params ###
        #######################

        # Possibly correct user inputs:
        if not isinstance(which_ids, list) and isinstance(which_ids, str):
            # If user did not put the word into a list...
            which_ids = [which_ids]

        # Check type:
        utils.check_type_parameter('which_ids', which_ids, list)

        # Check if either point or subc_id or both lon and lat are provided:
        utils.params_point_or_lonlat_or_subcid(point, lon, lat, subc_id)

        # Infer result_format from inputs, if not specified:
        if result_format is None:
            result_format = 'geojson' if point is not None else 'json'

        # Check result format:
        if result_format != 'json' and result_format != 'geojson':
            err_msg = f"Invalid result format: '{result_format}'. Expected 'json' or 'geojson'"
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

        # Check result format:
        if result_format == 'geojson' and point is None and lon is None and lat is None:
            err_msg = f"Cannot return GeoJSON without any coordinates. Please request plain 'json' or provide coordinates."
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

        # Check ids:
        possible_ids = ['subc_id', 'basin_id', 'reg_id']
        if not all([some_id in possible_ids for some_id in which_ids]):
            err_msg = f"The requested ids have to be one or several of: {possible_ids} (you provided {which_ids})"
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)

        ########################
        ### Retrieve results ###
        ########################

        # Possible results:
        subc_id = subc_id or None
        basin_id = None
        reg_id = None

        # If GeoJSON point is given, get coordinates:
        if point is not None:
            lon, lat = point.get('coordinates') or point['geometry']['coordinates']

        try:
            # Special case: User did not provide lon, lat but subc_id ...
            if subc_id is not None:
                LOGGER.debug('Special case: User provided a subc_id...')
                basin_id, reg_id = basic_queries.get_basinid_regid(
                    conn, LOGGER, subc_id = subc_id)
                LOGGER.debug(f'Special case: Returning reg_id ({reg_id}), basin_id ({basin_id}).')
                subc_id = subc_id

            # Normal case: User provided lon, lat:
            elif 'subc_id' in which_ids:
                LOGGER.log(logging.TRACE, f'Getting subc_id for lon, lat: {lon}, {lat}')
                subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon, lat)
                LOGGER.debug(f'FOUND: {subc_id}, {basin_id}, {reg_id}')

            elif 'basin_id' in which_ids:
                LOGGER.log(logging.TRACE, f'Getting basin_id for lon, lat: {lon}, {lat}')
                basin_id, reg_id = basic_queries.get_basinid_regid(
                    conn, LOGGER, lon, lat)

            elif 'reg_id' in which_ids:
                LOGGER.log(logging.TRACE, f'Getting reg_id for lon, lat: {lon}, {lat}')
                reg_id = basic_queries.get_regid(
                    conn, LOGGER, lon, lat)

        except exc.GeoFreshNoResultException as e:
            # TODO: Improve! What I don't like about this: This should not be an exception, but probably
            # quite a normal case...
            LOGGER.debug(f'Caught this: {e}, adding site_id: %{site_id}')
            if site_id is not None:
                err_msg = f'{e} ({site_id})'
                raise exc.GeoFreshNoResultException(err_msg)
            else:
                raise exc.GeoFreshNoResultException(e)


        ###################################
        ### Package and return results: ###
        ###################################

        # Assemble result dict:
        res = {}
        if subc_id is not None:
            res['subc_id'] = subc_id
        if basin_id is not None:
            res['basin_id'] = basin_id
        if reg_id is not None:
            res['reg_id'] = reg_id
        if site_id is not None:
            res['site_id'] = site_id # TODO: use colname_site_id ?

        # Return as plain json:
        if result_format == 'json':
            output_json = {'ids': res}

        # Return as GeoJSON:
        elif result_format == 'geojson':

            if point is not None:
                if point['type'] == 'Feature':
                    point['properties'].update(res)
                    output_json = point
                elif point['type'] == 'Point':
                    output_json = {
                        "type": "Feature",
                        "properties": res,
                        "geometry": point
                    }
                else:
                    err_msg = f"Unknown GeoJSON type: '{point['type']}'."
                    LOGGER.error(err_msg)
                    raise ProcessorExecuteError(err_msg)

            elif lon is not None and lat is not None:
                output_json = {
                    "type": "Feature",
                    "properties": res,
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    }
                }

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        # In this case, storing a JSON file is totally overdone! But for consistency's sake...
        return self.return_results('ids', requested_outputs, output_df=None, output_json=output_json, comment=comment)



if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-local-ids'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Request all three ids...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
        "lon": 9.931555,
        "lat": 54.695070,
        "which_ids": ["subc_id", "basin_id", "reg_id"],
        "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 2: Request only reg_id...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
        "lon": 9.931555,
        "lat": 54.695070,
        "which_ids": ["reg_id"],
        "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 3: Request only reg_id, as string...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
        "lon": 9.931555,
        "lat": 54.695070,
        "which_ids": "reg_id",
        "comment": "test3"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    print('TEST CASE 4: Request only basin_id...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
        "lon": 9.931555,
        "lat": 54.695070,
        "which_ids": "basin_id",
        "comment": "test4"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 5: Request all ids, knowing the subc_id...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
        "subc_id": 506250459,
        "which_ids": ["subc_id", "basin_id", "reg_id"],
        "comment": "test5"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 6: Request all three ids as JSON file...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
        "lon": 9.931555,
        "lat": 54.695070,
        "which_ids": ["subc_id", "basin_id", "reg_id"],
        "comment": "test6"
      },
      "outputs": {
        "transmissionMode": "reference"
      }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 7: Will fail: Format of input...', end="", flush=True)  # no newline
    payload = {
      "inputs": {
        "lon": "9.931555",
        "lat": 54.695070,
        "which_ids": ["subc_id", "basin_id", "reg_id"],
        "comment": "test7"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        print(f'TEST CASE 7: EXPECTED: {e.response.json()["description"]}')


    print('TEST CASE 8: Input point...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "point": {
                "type": "Point",
                "coordinates": [9.931555, 54.695070]
            },
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test8"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    print('TEST CASE 9: Input feature...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "point": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [9.931555, 54.695070]
                },
                "properties": {
                    "bla": "blaaaa"
                }
            },
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test9"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 10: Will Fail: Input not point...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "point": {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": []
                }
            },
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test10"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        print(f'TEST CASE 10: EXPECTED: {e.response.json()["description"]}')


    print('TEST CASE 11: Input feature, output json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "point": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [9.931555, 54.695070]
                }
            },
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "result_format": "json",
            "comment": "test11"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
