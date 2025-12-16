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
import pygeoapi.process.aqua90m.geofresh.get_polygons as get_polygons
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

# Request a FeatureCollection, based on a basin_id:
# Output: Polygon (FeatureCollection)
curl -X POST https://$PYSERVER/processes/get-basin-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": false,
    "comment": "close to bremerhaven"
    }
}'

# Request a simple GeometryCollection, based on a basin_id
# Output: Polygon (GeometryCollection)
curl -X POST "https://$PYSERVER/processes/get-basin-polygon/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": true,
    "comment": "close to bremerhaven"
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class BasinPolygonGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None
        self.process_id = self.metadata['id']
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
        return f'<BasinPolygonGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.debug(f'Start execution: {self.process_id} (job {self.job_id})')
        LOGGER.debug(f'Inputs: {data}')
        LOGGER.log(logging.TRACE, f'Requested outputs: {outputs}')

        try:
            conn = get_connection_object_config(self.config)
            res = self._execute(data, outputs, conn)
            LOGGER.debug(f'Finished execution: {self.process_id} (job {self.job_id})')
            LOGGER.log(logging.TRACE, 'Closing connection...')
            conn.close()
            LOGGER.log(logging.TRACE, 'Closing connection... Done.')
            return res

        except psycopg2.Error as e3:
            conn.close()
            err = f"{type(e3).__module__.removesuffix('.errors')}:{type(e3).__name__}: {str(e3).rstrip()}"
            error_message = f'Database error: {err} ({str(e3)}'
            LOGGER.error(error_message)
            raise ProcessorExecuteError(user_msg = error_message)

        except Exception as e:
            conn.close()
            LOGGER.error(f'During process execution, this happened: {e}')
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        basin_id = data.get('basin_id', None)
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', False)

        # Check type:
        utils.mandatory_parameters(dict(basin_id=basin_id))
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Get basin geometry:
        reg_id = basic_queries.get_regid_from_basinid(conn, LOGGER, basin_id)
        LOGGER.debug(f'Now, getting polygon for basin_id: {basin_id}')
        geojson_item = None

        if geometry_only:
            geojson_item = get_polygons.get_basin_polygon(conn, basin_id, reg_id, make_feature=False)
        else:
            geojson_item = get_polygons.get_basin_polygon(conn, basin_id, reg_id, make_feature=True)

        if comment is not None:
            geojson_item['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        if utils.return_hyperlink('polygon', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('polygon', geojson_item,
                self.metadata, self.job_id,
                self.download_dir,
                self.download_url)
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', geojson_item
