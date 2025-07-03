import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.snapping_strahler as snapping_strahler
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config

'''

# Request a simple GeometryCollection (Point):
curl -X POST "http://localhost:5000/processes/get-snapped-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "true",
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a FeatureCollection (Point):
curl -X POST "http://localhost:5000/processes/get-snapped-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "strahler": 3,
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


class SnappedPointsStrahlerGetter(BaseProcessor):

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
        return f'<SnappedPointsStrahlerGetter> {self.name}'


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
        lon = float(data.get('lon'))
        lat = float(data.get('lat'))
        strahler = float(data.get('strahler'))
        geometry_only = data.get('geometry_only', 'false')
        comment = data.get('comment') # optional

        # Parse booleans
        geometry_only = (geometry_only.lower() == 'true')

        # Get reg_id, basin_id, subc_id
        LOGGER.info('START: Getting snapped point for lon, lat: %s, %s' % (lon, lat))
        subc_id_before_snap, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
            conn, LOGGER, lon, lat) # TODO WIP We only need basin and region!

        # Return geometry only:
        if geometry_only:

            # Get snapped point:
            LOGGER.debug('... Now, getting snapped point for strahler order %s (as simple geometry)' % strahler)
            snappedpoint_simplegeom = snapping_strahler.get_snapped_point_geometry_coll(conn, lon, lat, strahler, basin_id, reg_id)

            if comment is not None:
                snappedpoint_simplegeom['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('snapped_point', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('snapped_point', snappedpoint_simplegeom,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', snappedpoint_simplegeom


        # Return Feature, incl. ids, strahler and original lonlat:
        if not geometry_only:

            # Get snapped point:
            LOGGER.debug('... Now, getting snapped point for subc_id (as feature): %s' % subc_id)
            snappedpoint_feature_coll = snapping_strahler.get_snapped_point_feature_coll(conn, lon, lat, strahler, basin_id, reg_id)

            snappedpoint_feature_coll['subc_id_before_snapping'] = subc_id_before_snap

            if comment is not None:
                snappedpoint_feature_coll['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('snapped_point', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('snapped_point', snappedpoint_feature_coll,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', snappedpoint_feature

