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
#import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.outlets as outlets
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
curl -X POST https://${PYSERVER}/processes/get-outlets-for-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 3,
    "add_geometry": false,
    "comment": "near helsinki",
    "polygon": {
      "type": "Polygon",
      "coordinates": [
        [
          [ 24.99422594742927, 60.122188238921],
          [ 24.99422594742927, 60.287391694733],
          [ 24.52403906370872, 60.287391694733],
          [ 24.52403906370872, 60.122188238921],
          [ 24.99422594742927, 60.122188238921]
        ]
      ]
    }
  }
}'

curl -X POST https://${PYSERVER}/processes/get-outlets-for-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 3,
    "add_geometry": true,
    "comment": "near helsinki",
    "polygon": {
      "type": "Polygon",
      "coordinates": [
        [
          [ 24.99422594742927, 60.122188238921],
          [ 24.99422594742927, 60.287391694733],
          [ 24.52403906370872, 60.287391694733],
          [ 24.52403906370872, 60.122188238921],
          [ 24.99422594742927, 60.122188238921]
        ]
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



class OutletGetter(BaseProcessor):

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
        return f'<OutletGetter> {self.name}'


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

    def _execute(self, data, requested_outputs, conn):

        # User inputs
        # GeoJSON, posted directly
        polygon_geojson = data.get('polygon', None)
        # GeoJSON, to be downloaded via URL:
        polygon_geojson_url = data.get('polygon_geojson_url', None)
        min_strahler = data.get('min_strahler')
        add_geometry = data.get('add_geometry', False)
        comment = data.get('comment') # optional

        ## Download GeoJSON if user provided URL:
        if polygon_geojson_url is not None:
            polygon_geojson = utils.download_geojson(polygon_geojson_url)
            LOGGER.debug(f'Downloaded GeoJSON: {polygon_geojson}')

        if add_geometry:
            featurecoll = outlets.get_outlet_streamsegments_in_polygon(conn,
                polygon_geojson,
                min_strahler=min_strahler
            )
            LOGGER.debug(f'Found Feature coll...')
            output_json = featurecoll
        else:
            subcids = outlets.get_outlet_subcids_in_polygon(conn,
                polygon_geojson,
                min_strahler=min_strahler
            )
            LOGGER.debug(f'Found subcids: {subcids}')
            subcids = {
                "subc_ids" : subcids
            }
            output_json = subcids


        ################
        ### Results: ###
        ################


        if comment is not None:
            output_json['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        if utils.return_hyperlink('outlets', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('outlets',
                output_json, self.metadata, self.job_id,
                self.config['download_dir'],
                self.config['download_url'])
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', output_json

