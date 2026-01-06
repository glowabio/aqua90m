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
import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
import pygeoapi.process.aqua90m.geofresh.bbox as bbox
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a simple Geometry (Polygon) (just one, not a collection):
# Tested: 2026-01-06
curl -X POST https://${PYSERVER}/processes/get-upstream-bbox/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": true,
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a Feature (Polygon) (just one, not a collection):
# Tested: 2026-01-06
curl -X POST https://${PYSERVER}/processes/get-upstream-bbox/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": false,
    "add_upstream_ids": true,
    "comment": "schlei-bei-rabenholz"
    }
}'

# Large: In the middle of river Elbe: 53.537158298376575, 9.99475350366553
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class UpstreamBboxGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment', None) # optional
        add_upstream_ids = data.get('add_upstream_ids', False)
        geometry_only = data.get('geometry_only', False)

        # Check if boolean:
        utils.is_bool_parameters(dict(
            add_upstream_ids=add_upstream_ids,
            geometry_only=geometry_only
        ))

        # Check if either subc_id or both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon, lat, subc_id)

        # Overall goal: Get the upstream stream segments!
        LOGGER.info(f'START: Getting upstream bbox for lon, lat: {lon}, {lat} (or subc_id {subc_id})')

        # Get reg_id, basin_id, subc_id
        if subc_id is not None:
            # (special case: user provided subc_id instead of lonlat!)
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        else:
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)

        # Get upstream ids:
        upstream_ids = upstream_subcids.get_upstream_catchment_ids_incl_itself(
            conn, subc_id, basin_id, reg_id)

        if geometry_only:

            # Get bounding box:
            bbox_simplegeom = bbox.get_bbox_simplegeom(
                conn, upstream_ids, basin_id, reg_id)
            # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('bbox', requested_outputs, output_df=None, output_json=bbox_simplegeom, comment=comment)


        if not geometry_only:

            # Get bounding box:
            # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
            bbox_feature = bbox.get_bbox_feature(
                conn, upstream_ids, basin_id, reg_id, add_subc_ids = add_upstream_ids)


            # Add some info to the Feature:
            # TODO: Should we include the requested lon and lat? Maybe as a point? Then FeatureCollection?
            bbox_feature["description"] = f"Bounding box of the upstream catchment of subcatchment {subc_id}"
            bbox_feature["bbox_of_upstream_catchment_of"] = subc_id

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('bbox', requested_outputs, output_df=None, output_json=bbox_feature, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    process_id = 'get-upstream-bbox'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Request simple Geometry (Polygon)...', end="", flush=True)  # no newline
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

    print('TEST CASE 2: Request simple Feature (Polygon)...', end="", flush=True)  # no newline
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

