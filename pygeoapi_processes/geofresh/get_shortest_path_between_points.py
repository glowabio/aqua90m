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
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a GeometryCollection (LineStrings):
# Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon_start": 9.937520027160646,
    "lat_start": 54.69422745526058,
    "lon_end": 9.9217,
    "lat_end": 54.6917,
    "geometry_only": true
    }
}'

# Request a FeatureCollection (LineStrings):
# Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-between-points/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon_start": 9.937520027160646,
    "lat_start": 54.69422745526058,
    "lon_end": 9.9217,
    "lat_end": 54.6917,
    "geometry_only": false,
    "add_segment_ids": true,
    "comment": "test"
    }
}'
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class ShortestPathBetweenPointsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon_start = data.get('lon_start', None)
        lat_start = data.get('lat_start', None)
        lon_end = data.get('lon_end', None)
        lat_end = data.get('lat_end', None)
        subc_id_start = data.get('subc_id_start', None) # optional, need either lonlat OR subc_id
        subc_id_end = data.get('subc_id_end', None)     # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        add_segment_ids = data.get('add_segment_ids', True)
        geometry_only = data.get('geometry_only', False)

        ## Check if boolean:
        utils.is_bool_parameters(dict(
            add_segment_ids=add_segment_ids,
            geometry_only=geometry_only))

        # Overall goal: Get the dijkstra shortest path (as linestrings)!
        LOGGER.info('START: Getting dijkstra shortest path for lon %s, lat %s (or subc_id %s) to lon %s, lat %s (or subc_id %s)' % (
            lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end))

        # Get reg_id, basin_id, subc_id
        # Point 1:
        if subc_id_start is not None:
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id=subc_id_start)
        else:
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon_start, lat_start)
        # Point 2:
        if subc_id_end is not None:
            subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id=subc_id_end)
        else:
            subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon_end, lat_end)

        # Check if same region and basin?
        # TODO: Can we route via the sea then??
        if not reg_id1 == reg_id2:
            err_msg = 'Start and end are in different regions (%s and %s) - this cannot work.' % (reg_id1, reg_id2)
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        if not basin_id1 == basin_id2:
            err_msg = 'Start and end are in different basins (%s and %s) - this cannot work.' % (basin_id1, basin_id2)
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        # Get subc_ids of the whole connection...
        # TODO: From here on, I think it is exactly the same code as getting downstream
        # to sea! So: Modularize and import!
        LOGGER.debug('Getting network connection for subc_id: start = %s, end = %s' % (subc_id1, subc_id2))
        segment_ids = routing.get_dijkstra_ids_one_to_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)

        # Get geometry only:
        if geometry_only:
            geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(
                conn, segment_ids, basin_id1, reg_id1)

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('connecting_path', requested_outputs, output_df=None, output_json=geometry_coll, comment=comment)


        # Get FeatureCollection
        if not geometry_only:

            feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(
                conn, segment_ids, basin_id1, reg_id1)

            # Add some info to the FeatureCollection:
            # TODO: Should we include the requested lon and lat? Maybe as a point?
            feature_coll["description"] = "Connecting path between %s and %s" % (subc_id1, subc_id2)
            feature_coll["start_subc_id"] = subc_id1 # TODO how to name the start point of routing?
            feature_coll["target_subc_id"] = subc_id2 # TODO how to name the end point of routing?
            if add_segment_ids:
                feature_coll["subc_ids"] = segment_ids

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            return self.return_results('connecting_path', requested_outputs, output_df=None, output_json=feature_coll, comment=comment)
