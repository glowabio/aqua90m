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
import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
import pygeoapi.process.aqua90m.geofresh.get_polygons as get_polygons
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a GeometryCollection (Polygons):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-upstream-subcatchments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": true,
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a FeatureCollection (Polygons):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-upstream-subcatchments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": false,
    "add_upstream_ids": true,
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a FeatureCollection (Polygons) as URL:
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-upstream-subcatchments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": false,
    "add_upstream_ids": true,
    "comment": "schlei-near-rabenholz"
    },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

# Large: Mitten in der Elbe: 53.537158298376575, 9.99475350366553
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class UpstreamSubcatchmentGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        ## User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', False)
        add_upstream_ids = data.get('add_upstream_ids', False)

        # Check if boolean:
        utils.is_bool_parameters(dict(
            add_upstream_ids=add_upstream_ids,
            geometry_only=geometry_only
        ))

        # Check if either subc_id or both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon, lat, subc_id)

        # Overall goal: Get the upstream polygons (individual ones)
        LOGGER.info(f'START: Getting upstream polygons (individual ones) for lon, lat: {lon}, {lat} (or subc_id {subc_id})')

        # Get reg_id, basin_id, subc_id
        if subc_id is not None:
            # (special case: user provided subc_id instead of lonlat!)
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        else:
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)

        # Get upstream_ids
        upstream_ids = upstream_subcids.get_upstream_catchment_ids_incl_itself(
            conn, subc_id, basin_id, reg_id)

        # Get geometry only:
        if geometry_only:
            LOGGER.debug(f'...Getting upstream catchment polygons for subc_id: {subc_id}')
            geometry_coll = get_polygons.get_subcatchment_polygons_geometry_coll(
                conn, upstream_ids, basin_id, reg_id)
            LOGGER.debug('END: Received GeometryCollection: %s' % str(geometry_coll)[0:50])

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('polygons', requested_outputs, output_df=None, output_json=geometry_coll, comment=comment)

        # Get FeatureCollection
        if not geometry_only:
            LOGGER.debug(f'...Getting upstream catchment polygons for subc_id: {subc_id}')
            feature_coll = get_polygons.get_subcatchment_polygons_feature_coll(
                conn, upstream_ids, basin_id, reg_id, add_upstream_ids)
            LOGGER.debug('END: Received FeatureCollection: %s' % str(feature_coll)[0:50])

            feature_coll['description'] = f"Upstream subcatchments of subcatchment {subc_id}."
            feature_coll['upstream_catchment_of'] = subc_id

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('polygons', requested_outputs, output_df=None, output_json=feature_coll, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-upstream-subcatchments'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Request GeometryCollection (Polygons)...', end="", flush=True)  # no newline
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

    print('TEST CASE 2: Request FeatureCollection (Polygons)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 9.931555,
            "lat": 54.695070,
            "geometry_only": False,
            "add_upstream_ids": True,
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)

    print('TEST CASE 2: Request FeatureCollection (Polygons)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 9.931555,
            "lat": 54.695070,
            "geometry_only": False,
            "add_upstream_ids": True,
            "comment": "test2"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

