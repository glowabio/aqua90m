import logging
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.get_env90m as get_env90m
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config

'''
# Input points: GeoJSON
curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "subc_ids": [506250459, 506251015, 506251126, 506251712],
        "variables": ["bio1", "bio7", "c20", "flow_ltm", "clyppt", "shreve", "elev"],
        "comment": "no idea where this is"
    }
}'
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class Env90mGetter(BaseProcessor):

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
        return f'<Env90mGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the env90m for a list of subcatchments..."')
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

        # User inputs: JSON
        subc_ids = data.get("subc_ids")
        variables = data.get("variables")
        comment = data.get('comment') # optional

        ########################################
        ### Get Env90m info for each subc_id ###
        ########################################

        reg_ids = set()
        for subc_id in subc_ids:
            basin_id, reg_id = basic_queries.get_basinid_regid(conn, subc_id)
            reg_ids.add(reg_id)

        if len(reg_ids) > 1:
            err_msg = "WIP: The subcatchments fall into various regional units (reg_ids: %s). Not supported yet." % reg_ids
            LOGGER.warning(err_msg)
            raise ValueError(err_msg)

        reg_id = reg_ids.pop()
        output_json = get_env90m.get_env90m_variables_by_subcid(conn, subc_ids, reg_id, variables)

        ################
        ### Results: ###
        ################

        if output_json is not None:
            output_json['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        # In this case, storing a JSON file is totally overdone! But for consistency's sake...
        if utils.return_hyperlink('env90m', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('env90m', output_json,
                self.metadata, self.job_id,
                self.config['download_dir'],
                self.config['download_url'])
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', output_json

