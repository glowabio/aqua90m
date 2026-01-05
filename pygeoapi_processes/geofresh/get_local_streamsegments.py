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
# Request a simple Geometry (LineString) (just one, not a collection):
# Tested: 2026-02-01
curl -X POST https://${PYSERVER}/processes/get-local-streamsegments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": true,
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a Feature (LineString) (just one, not a collection):
# Tested: 2026-02-01
curl -X POST https://${PYSERVER}/processes/get-local-streamsegments/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": false,
    "comment": "schlei-near-rabenholz"
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalStreamSegmentsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        geometry_only = data.get('geometry_only', False)
        comment = data.get('comment') # optional

        # Check if either subc_id or both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon, lat, subc_id)
        # Check type:
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Get reg_id, basin_id, subc_id
        if subc_id is not None:
            # (special case: user provided subc_id instead of lonlat!)
            LOGGER.info('Retrieving stream segment for subc_id %s' % subc_id)
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        else:
            LOGGER.info('Retrieving stream segment for lon, lat: %s, %s' % (lon, lat))
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)
        
        # Get only geometry:
        if geometry_only:

            LOGGER.debug('Now, getting stream segment for subc_id: %s' % subc_id)
            geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, [subc_id], basin_id, reg_id)
            streamsegment_simplegeom = geometry_coll["geometries"][0]

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('stream_segment', requested_outputs, output_df=None, output_json=streamsegment_simplegeom, comment=comment)


        # Get Feature:
        if not geometry_only:

            LOGGER.debug('Now, getting stream segment (incl. strahler order) for subc_id: %s' % subc_id)
            feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(conn, [subc_id], basin_id, reg_id)
            streamsegment_feature = feature_coll["features"][0]

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('stream_segment', requested_outputs, output_df=None, output_json=streamsegment_feature, comment=comment)

