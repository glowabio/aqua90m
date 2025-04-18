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
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request some JSON, to be improved (TODO):
curl -X POST "http://localhost:5000/processes/get-shortest-path-between-points-plural/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points": {
      "type": "MultiPoint",
      "coordinates": [
        [9.937520027160646, 54.69422745526058],
        [9.9217, 54.6917],
        [9.9312, 54.6933]
      ]
    },
    "comment": "located in schlei area",
    "geometry_only": "todo"
  }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class ShortestPathBetweenPointsGetterPlural(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None
        self.config = None

        # Set config:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            self.config = json.load(config_file)
            self.download_dir = self.config['download_dir']
            self.download_url = self.config['download_url']


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<ShortestPathBetweenPointsGetterPlural> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.debug('Start execution: %s (job %s)' % (self.metadata['id'], self.job_id))
        LOGGER.debug('Inputs: %s' % data)
        LOGGER.log(logging.TRACE, 'Requested outputs: %s' % outputs)

        try:
            conn = get_connection_object_config(self.config)
            res = self._execute(data, outputs, conn)
            LOGGER.debug('Finished execution: %s (job %s)' % (self.metadata['id'], self.job_id))
            LOGGER.log(logging.TRACE, 'Closing connection...')
            conn.close()
            LOGGER.log(logging.TRACE, 'Closing connection... Done.')
            return res

        except psycopg2.Error as e3:
            conn.close()
            err = f"{type(e3).__module__.removesuffix('.errors')}:{type(e3).__name__}: {str(e3).rstrip()}"
            error_message = 'Database error: %s (%s)' % (err, str(e3))
            LOGGER.error(error_message)
            raise ProcessorExecuteError(user_msg = error_message)

        except Exception as e:
            conn.close()
            LOGGER.error('During process execution, this happened: %s' % e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?
            #TODO OR: raise ProcessorExecuteError(e, user_msg=e.message)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        points = data.get('points', None)
        #lon_start = data.get('lon_start', None)
        #lat_start = data.get('lat_start', None)
        #lon_end = data.get('lon_end', None)
        #lat_end = data.get('lat_end', None)
        #subc_id_start = data.get('subc_id_start', None) # optional, need either lonlat OR subc_id
        #subc_id_end = data.get('subc_id_end', None)     # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        add_segment_ids = data.get('add_segment_ids', 'true')
        geometry_only = data.get('geometry_only', 'false')

        # Parse booleans
        add_segment_ids = (add_segment_ids.lower() == 'true')
        geometry_only = (geometry_only.lower() == 'true')

        # Overall goal: Get the dijkstra shortest path (as linestrings)!
        #LOGGER.info('START: Getting dijkstra shortest path for lon %s, lat %s (or subc_id %s) to lon %s, lat %s (or subc_id %s)' % (
        #    lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end))

        # Get reg_id, basin_id, subc_id
        all_subc_ids = []
        all_reg_ids = []
        all_basin_ids = []
        for lon, lat in points['coordinates']:
            # TODO: Loop may not be most efficient!
            LOGGER.debug('Now getting subc_id, basin_id, reg_id for lon %s, lat %s' % (lon, lat))
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)
            all_subc_ids.append(subc_id)
            all_reg_ids.append(reg_id)
            all_basin_ids.append(basin_id)

        # Check if same region and basin?
        # TODO: Can we route via the sea then??
        if len(set(all_reg_ids)) == 1:
            reg_id = all_reg_ids[0]
        else:
            err_msg = 'The input points are in different regions (%s) - this cannot work.' % set(all_reg_ids)
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        if len(set(all_basin_ids)) == 1:
            basin_id = all_basin_ids[0]
        else:
            err_msg = 'The input points are in different basins (%s) - this cannot work.' % set(all_basin_ids)
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        #if not reg_id1 == reg_id2:
        #    err_msg = 'Start and end are in different regions (%s and %s) - this cannot work.' % (reg_id1, reg_id2)
        #    LOGGER.warning(err_msg)
        #    raise ProcessorExecuteError(user_msg=err_msg)

        #if not basin_id1 == basin_id2:
        #    err_msg = 'Start and end are in different basins (%s and %s) - this cannot work.' % (basin_id1, basin_id2)
        #    LOGGER.warning(err_msg)
        #    raise ProcessorExecuteError(user_msg=err_msg)

        # Get subc_ids of the whole connection...
        #LOGGER.debug('Getting network connection for subc_id: start = %s, end = %s' % (subc_id1, subc_id2))
        #segment_ids = routing.get_dijkstra_ids_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)
        some_json_result = routing.get_dijkstra_ids_many(conn, all_subc_ids, reg_id, basin_id)

        if comment is not None:
            some_json_result['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        if utils.return_hyperlink('paths_matrix', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('paths_matrix', some_json_result,
                self.metadata, self.job_id,
                self.download_dir,
                self.download_url)
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', some_json_result

        # Get geometry only:
        #if geometry_only:
        #    geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(
        #        conn, segment_ids, basin_id1, reg_id1)

        #    if comment is not None:
        #        geometry_coll['comment'] = comment

        #    if self.return_hyperlink('connecting_path', requested_outputs):
        #        return 'application/json', self.store_to_json_file('connecting_path', geometry_coll)
        #    else:
        #        return 'application/json', geometry_coll


        # Get FeatureCollection
        #if not geometry_only:

        #    feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(
        #        conn, segment_ids, basin_id1, reg_id1, add_subc_ids = add_segment_ids)

        #    # Add some info to the FeatureCollection:
        #    feature_coll["description"] = "Connecting path between %s and %s" % (subc_id1, subc_id2)
        #    feature_coll["start_subc_id"] = subc_id1 # TODO how to name the start point of routing?
        #    feature_coll["target_subc_id"] = subc_id2 # TODO how to name the end point of routing?
        #    # TODO: Should we include the requested lon and lat? Maybe as a point?

        #    if comment is not None:
        #        feature_coll['comment'] = comment

        #    if self.return_hyperlink('connecting_path', requested_outputs):
        #        return 'application/json', self.store_to_json_file('connecting_path', feature_coll)
        #    else:
        #        return 'application/json', feature_coll

