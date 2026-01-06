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
import pygeoapi.process.aqua90m.geofresh.snapping as snapping
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config

'''

# Request a simple Geometry (Point) (just one, not a collection):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-snapped-points/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": true,
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a Feature (Point) (just one, not a collection):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-snapped-points/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": false,
    "comment": "schlei-near-rabenholz"
    }
}'

# TODO: FUTURE: If we ever snap to stream segments outside of the immediate subcatchment,
# need to adapt some stuff in this process...
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class SnappedPointsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon = float(data.get('lon'))
        lat = float(data.get('lat'))
        geometry_only = data.get('geometry_only', False)
        comment = data.get('comment') # optional

        # Check if both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon, lat, None)

        # Check if boolean:
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Get reg_id, basin_id, subc_id
        LOGGER.info(f'START: Getting snapped point for lon, lat: {lon}, {lat}')
        subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
            conn, LOGGER, lon, lat)

        # Return geometry only:
        if geometry_only:

            # Get snapped point:
            LOGGER.debug(f'... Now, getting snapped point for subc_id (as simple geometry): {subc_id}')
            snappedpoint_simplegeom = snapping.get_snapped_point_simplegeom(
                conn, lon, lat, subc_id, basin_id, reg_id)

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('snapped_point', requested_outputs, output_df=None, output_json=snappedpoint_simplegeom, comment=comment)

        # Return Feature, incl. ids, strahler and original lonlat:
        if not geometry_only:

            # Get snapped point:
            LOGGER.debug(f'... Now, getting snapped point for subc_id (as feature): {subc_id}')
            snappedpoint_feature = snapping.get_snapped_point_feature(
                conn, lon, lat, subc_id, basin_id, reg_id)

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('snapped_point', requested_outputs, output_df=None, output_json=snappedpoint_feature, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    process_id = 'get-snapped-points'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Request Geometry (Point)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 9.931555,
            "lat": 54.695070,
            "geometry_only": True,
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)

    print('TEST CASE 2: Request Feature (Point)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 9.931555,
            "lat": 54.695070,
            "geometry_only": False,
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)
