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

        # Check if boolean:
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Get reg_id, basin_id, subc_id
        LOGGER.info('START: Getting snapped point for lon, lat: %s, %s' % (lon, lat))
        subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
            conn, LOGGER, lon, lat)

        # Return geometry only:
        if geometry_only:

            # Get snapped point:
            LOGGER.debug('... Now, getting snapped point for subc_id (as simple geometry): %s' % subc_id)
            snappedpoint_simplegeom = snapping.get_snapped_point_simplegeom(
                conn, lon, lat, subc_id, basin_id, reg_id)

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('snapped_point', requested_outputs, output_df=None, output_json=snappedpoint_simplegeom, comment=comment)

        # Return Feature, incl. ids, strahler and original lonlat:
        if not geometry_only:

            # Get snapped point:
            LOGGER.debug('... Now, getting snapped point for subc_id (as feature): %s' % subc_id)
            snappedpoint_feature = snapping.get_snapped_point_feature(
                conn, lon, lat, subc_id, basin_id, reg_id)

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('snapped_point', requested_outputs, output_df=None, output_json=snappedpoint_feature, comment=comment)
