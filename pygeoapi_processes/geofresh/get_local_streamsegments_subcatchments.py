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
import pygeoapi.process.aqua90m.geofresh.get_polygons as get_polygons
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
Note:
TODO FIXME: This should be replaced by using the normal get_stream_segment.py with parameter add_subcatchment,
but then I need to change my test HTML client, which currently only can make different process calls
by using different process id, and not by adding parameters.

# Request a GeometryCollection (LineStrings):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-streamsegments-subcatchments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": true,
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a FeatureCollection (LineStrings):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-streamsegments-subcatchments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": false,
    "comment": "schlei-bei-rabenholz"
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalStreamSegmentSubcatchmentGetter(GeoFreshBaseProcessor):


    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        point = data.get('point', None)
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        geometry_only = data.get('geometry_only', False)
        comment = data.get('comment') # optional

        # Check type:
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Check if either point or subc_id or both lon and lat are provided:
        utils.params_point_or_lonlat_or_subcid(point, lon, lat, subc_id)

        # If GeoJSON point is given, get coordinates:
        if point is not None:
            lon, lat = point.get('coordinates') or point['geometry']['coordinates']

        # Get reg_id, basin_id, subc_id
        if subc_id is not None:
            # (special case: user provided subc_id instead of lonlat!)
            LOGGER.info('Getting stream segment and subcatchment for subc_id %s' % subc_id)
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        else:
            LOGGER.info('Getting stream segment and subcatchment for lon, lat: %s, %s' % (lon, lat))
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)

        # Return only geometry:
        if geometry_only:

            LOGGER.debug('... Now, getting stream segment for subc_id: %s' % subc_id)
            geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, [subc_id], basin_id, reg_id)
            streamsegment_simplegeom = geometry_coll["geometries"][0]

            LOGGER.debug('... Now, getting subcatchment polygon for subc_id: %s' % subc_id)
            geometry_coll = get_polygons.get_subcatchment_polygons_geometry_coll(conn, [subc_id], basin_id, reg_id)
            subcatchment_simplegeom = geometry_coll["geometries"][0]

            # Make GeometryCollection from both:
            geometry_coll = {
                "type": "GeometryCollection",
                "geometries": [streamsegment_simplegeom, subcatchment_simplegeom]
            }

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('stream_segment_subcatchment', requested_outputs, output_df=None, output_json=geometry_coll, comment=comment)

        # Return feature collection:
        if not geometry_only:

            LOGGER.debug('...Now, getting stream segment (incl. strahler order) for subc_id: %s' % subc_id)
            feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(conn, [subc_id], basin_id, reg_id)
            streamsegment_feature = feature_coll["features"][0]

            LOGGER.debug('... Now, getting subcatchment polygon for subc_id: %s' % subc_id)
            feature_coll = get_polygons.get_subcatchment_polygons_feature_coll(conn, [subc_id], basin_id, reg_id)
            subcatchment_feature = feature_coll["features"][0]

            # Make FeatureCollection from both:
            feature_coll = {
                "type": "FeatureCollection",
                "features": [streamsegment_feature, subcatchment_feature],
                "basin_id": basin_id,
                "reg_id": reg_id
            }

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('stream_segment_subcatchment', requested_outputs, output_df=None, output_json=feature_coll, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-local-streamsegments-subcatchments'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Request geometry_only...', end="", flush=True)  # no newline
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
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 2: Request full result...', end="", flush=True)  # no newline
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
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 3: Will fail: Wrong format...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 9.931555,
            "lat": 54.695070,
            "geometry_only": "false",
            "comment": "test3"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        print(f'TEST CASE 3: EXPECTED: {e.response.json()["description"]}')


    print('TEST CASE 4: Input Feature (Point)...', end="", flush=True)  # no newline
    payload = {
        "inputs":
            {
            "point": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [9.931555, 54.695070]
                }
            },
            "geometry_only": False,
            "comment": "test4"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)
    #print(f'RESP: {resp.json()}\n')
