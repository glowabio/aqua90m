import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.snapping as snapping
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config



'''
curl -X POST "http://localhost:5000/pygeoapi/processes/get-snapped-points-plural/execution" \
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
    }
  }
}'
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class SnappedPointsGetterPlural(BaseProcessor):

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
        return f'<SnappedPointsGetterPlural> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the snapped point coordinates..."')
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
        input_points_geojson = data.get('points')
        geometry_only = data.get('geometry_only', 'false')
        comment = data.get('comment') # optional

        # Parse booleans
        geometry_only = (geometry_only.lower() == 'true')

        if geometry_only:
            raise ProcessorExecuteError("Not implemented yet, please set geometry_only to False.")
            # TODO: Not implemented yet: Returning snapped points as simple geometry, instead of Feature.

        if not geometry_only:
            feature_coll = snapping.get_snapped_points_1(conn, input_points_geojson)
            LOGGER.debug('Received feature collection: %s' % feature_coll)

            # Add comment:
            if comment is not None:
                feature_coll['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('snapped_points', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('snapped_points', feature_coll,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', feature_coll

