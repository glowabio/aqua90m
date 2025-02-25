
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a simple Geometry (LineString) (just one, not a collection):
curl -X POST "http://localhost:5000/processes/get-local-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "true",
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a Feature (LineString) (just one, not a collection):
curl -X POST "http://localhost:5000/processes/get-local-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "false",
    "comment": "schlei-near-rabenholz"
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalStreamSegmentsGetter(BaseProcessor):

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
        return f'<LocalStreamSegmentsGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the stream segment..."')
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
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', 'false')

        # Parse booleans
        geometry_only = (geometry_only.lower() == 'true')

        LOGGER.info('Retrieving stream segment for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))
        subc_id, basin_id, reg_id = basic_queries.get_subc_id_basin_id_reg_id(
            conn, LOGGER, lon, lat, subc_id)
        
        # Get only geometry:
        if geometry_only:

            LOGGER.debug('Now, getting stream segment for subc_id: %s' % subc_id)
            geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, [subc_id], basin_id, reg_id)
            streamsegment_simplegeom = geometry_coll["geometries"][0]
        
            if comment is not None:
                streamsegment_simplegeom['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('stream_segment', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('stream_segment', streamsegment_simplegeom,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', streamsegment_simplegeom


        # Get Feature:
        if not geometry_only:

            LOGGER.debug('Now, getting stream segment (incl. strahler order) for subc_id: %s' % subc_id)
            feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(conn, [subc_id], basin_id, reg_id)
            streamsegment_feature = feature_coll["features"][0]

            if comment is not None:
                streamsegment_feature['properties']['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('stream_segment', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('stream_segment', streamsegment_feature,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', streamsegment_feature

