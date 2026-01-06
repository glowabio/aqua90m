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
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config

'''

# Request a GeometryCollection (LineStrings):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-upstream-streamsegments/execution \
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
curl -X POST https://${PYSERVER}/processes/get-upstream-streamsegments/execution \
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

# Large: In the middle of Elbe river: 53.537158298376575, 9.99475350366553

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class UpstreamStreamSegmentsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        add_upstream_ids = data.get('add_upstream_ids', False)
        geometry_only = data.get('geometry_only', False)

        # Check if boolean:
        utils.is_bool_parameters(dict(
            geometry_only=geometry_only,
            add_upstream_ids=add_upstream_ids
        ))

        # Check if either subc_id or both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon, lat, subc_id)

        # Overall goal: Get the upstream stream segments
        LOGGER.info(f'Getting upstream line segments for lon, lat: {lon}, {lat} (or subc_id {subc_id})')

        # Get reg_id, basin_id, subc_id
        if subc_id is not None:
            # (special case: user provided subc_id instead of lonlat!)
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        else:
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)

        # Get upstream ids
        upstream_ids = upstream_subcids.get_upstream_catchment_ids_incl_itself(
            conn, subc_id, basin_id, reg_id)

        # Cumulative length as JSON:
        # TODO: We could include this into the query for the FeatureCollection,
        # instead of querying the database twice. But for now, it works.
        LOGGER.debug("Querying for cumulative length...")
        cum_length_by_strahler = get_linestrings.get_accum_length_by_strahler(
            conn, upstream_ids, basin_id, reg_id)
        LOGGER.debug(f"Querying for cumulative length DONE: {cum_length_by_strahler}")

        # Log interesting cases:
        if len(upstream_ids) == 0:
            LOGGER.warning('No upstream ids. Cannot get upstream linestrings .')
        if len(upstream_ids) == 1 and subc_id == upstream_ids[0]:
            LOGGER.debug('Upstream catchments equals subcatchment!')


        # Get geometry only:
        if geometry_only:

            if len(upstream_ids) == 0:
                geometry_coll = {
                    "type": "GeometryCollection",
                    "geometries": []
                }
            else:
                LOGGER.debug(f'... Getting upstream catchment line segments for subc_id: {subc_id}')
                geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, upstream_ids, basin_id, reg_id)

            LOGGER.debug('END: Received GeometryCollection: %s' % str(geometry_coll)[0:50])

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('upstream_stream_segments', requested_outputs, output_df=None, output_json=geometry_coll, comment=comment)


        # Get FeatureCollection
        if not geometry_only:

            if len(upstream_ids) == 0:
                # Feature Collections can have empty array according to GeoJSON spec::
                # https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
                feature_coll = {
                    "type": "FeatureCollection",
                    "features": [],
                    "basin_id": basin_id,
                    "reg_id": reg_id,
                    "cumulative_length": 0,
                    "cumulative_length_by_strahler": 0
                }

            else:
                # Note: The feature collection contains the strahler order for each feature (each stream segment)
                LOGGER.debug(f'... Getting upstream catchment line segments for subc_id: {subc_id}')
                feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(
                    conn, upstream_ids, basin_id, reg_id)

            # Add some info to the FeatureCollection:
            feature_coll["part_of_upstream_catchment_of"] = subc_id
            feature_coll["cumulative_length"] = cum_length_by_strahler["all_strahler_orders"],
            feature_coll["cumulative_length_by_strahler"] = cum_length_by_strahler
            if add_upstream_ids:
                feature_coll["upstream_ids"] = upstream_ids

            LOGGER.debug('END: Received FeatureCollection: %s' % str(feature_coll)[0:50])

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('upstream_stream_segments', requested_outputs, output_df=None, output_json=feature_coll, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    process_id = 'get-upstream-streamsegments'
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
