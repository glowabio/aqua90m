
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a GeometryCollection (LineStrings):
curl -X POST "http://localhost:5000/processes/get-shortest-path-to-outlet/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.937520027160646,
    "lat": 54.69422745526058,
    "geometry_only": "true",
    "comment": "bla"
    }
}'

# Request a FeatureCollection (LineStrings):
curl -X POST "http://localhost:5000/processes/get-shortest-path-to-outlet/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.937520027160646,
    "lat": 54.69422745526058,
    "geometry_only": "false",
    "add_downstream_ids": "true",
    "comment": "bla"
    }
}'
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class ShortestPathToOutletGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None
        self.config = None

        # Set config:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            self.config = json.load(config_file)

    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<ShortestPathToOutletGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the dijkstra shortest path to sea..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        try:
            conn = get_connection_object_config(self.config)
            res = self._execute(data, outputs, conn)

            LOGGER.debug('Closing connection...')
            conn.close()
            LOGGER.debug('Closing connection... Done.')

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


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon_start = data.get('lon', None)
        lat_start = data.get('lat', None)
        subc_id1 = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', 'false')
        add_downstream_ids = data.get('add_downstream_ids', 'true')

        # Parse booleans
        geometry_only = (geometry_only.lower() == 'true')
        add_downstream_ids = (add_downstream_ids.lower() == 'true')

        # Overall goal: Get the dijkstra shortest path (as linestrings)!
        LOGGER.info('START: Getting dijkstra shortest path for lon %s, lat %s (or subc_id %s) to sea' % (
            lon_start, lat_start, subc_id1))
        subc_id1, basin_id1, reg_id1 = basic_queries.get_subc_id_basin_id_reg_id(
            conn, LOGGER, lon_start, lat_start, subc_id1)

        # Outlet has minus basin_id as subc_id!
        subc_id2 = -basin_id1

        # Get subc_ids of the whole connection...
        LOGGER.debug('Getting network connection for subc_id: start = %s, end = %s' % (subc_id1, subc_id2))
        segment_ids = routing.get_dijkstra_ids_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)

        # Get geometry only:
        if geometry_only:
            geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(
                conn, segment_ids, basin_id1, reg_id1)

            if comment is not None:
                geometry_coll['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('downstream_path', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('downstream_path', geometry_coll,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', geometry_coll

        # Get FeatureCollection
        if not geometry_only:

            feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(
                conn, segment_ids, basin_id1, reg_id1, add_subc_ids = add_downstream_ids)
        
            # Add some info to the FeatureCollection:
            feature_coll["description"] = "Downstream path from subcatchment %s to the outlet of its basin." % subc_id1
            feature_coll["subc_id"] = subc_id1 # TODO how to name the point from where we route to outlet?
            feature_coll["outlet_id"] = subc_id2
            feature_coll["downstream_path_of"] = subc_id1
            # TODO: Should we include the requested lon and lat? Maybe as a point?
            
            if comment is not None:
                feature_coll['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('downstream_path', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('downstream_path', feature_coll,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', feature_coll
