import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
import pygeoapi.process.aqua90m.utils.exceptions as exc
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

'''

# Request all ids
curl -X POST "http://localhost:5000/processes/get-local-ids/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": "subc_id, basin_id, reg_id",
    "comment": "schlei-near-rabenholz"
    }
}'

# Request only reg_id
curl -X POST "http://localhost:5000/processes/get-local-ids/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": "reg_id",
    "comment": "schlei-near-rabenholz"
    }
}'

# Request only basin_id
curl -X POST "http://localhost:5000/processes/get-local-ids/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "which_ids": "basin_id",
    "comment": "schlei-near-rabenholz"
    }
}'

# Special case: Request all ids, when we know the subc_id!
curl -X POST "http://localhost:5000/processes/get-local-ids/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_id": 506250459,
    "which_ids": "subc_id, basin_id, reg_id",
    "comment": "schlei-near-rabenholz"
    }
}'


'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalIdGetter(BaseProcessor):
    # TODO: This can replace get_local_subcids... BUt this is not plural yet.

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
        return f'<LocalIdGetter> {self.name}'


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
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        input_subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        site_id = data.get('site_id') # optional
        which_ids = data.get('which_ids', 'subc_id, basin_id, reg_id')
        which_ids = which_ids.replace(' ', '')
        which_ids = which_ids.split(',')

        possible_ids = ['subc_id', 'basin_id', 'reg_id']
        if not all([some_id in possible_ids for some_id in which_ids]):
            err_msg = "The requested ids have to be one or several of: %s (you provided %s)" % (possible_ids, which_ids)
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)

        # Possible results:
        subc_id = None
        basin_id = None
        reg_id = None

        # Special case: User did not provide lon, lat but subc_id ...
        try:
            if input_subc_id is not None:
                LOGGER.debug('Special case: User provided a subc_id...')
                basin_id, reg_id = basic_queries.get_basinid_regid(
                    conn, LOGGER, subc_id = input_subc_id)
                LOGGER.debug('Special case: Returning reg_id (%s), basin_id (%s).' % (reg_id, basin_id))
                subc_id = input_subc_id

            # Normal case: User provided lon, lat:
            elif 'subc_id' in which_ids:
                LOGGER.log(logging.TRACE, 'Getting subc_id for lon, lat: %s, %s' % (lon, lat))
                subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon, lat)
                LOGGER.debug('FOUND: %s %s %s' % (subc_id, basin_id, reg_id))

            elif 'basin_id' in which_ids:
                LOGGER.log(logging.TRACE, 'Getting basin_id for lon, lat: %s, %s' % (lon, lat))
                basin_id, reg_id = basic_queries.get_basinid_regid(
                    conn, LOGGER, lon, lat)

            elif 'reg_id' in which_ids:
                LOGGER.log(logging.TRACE, 'Getting reg_id for lon, lat: %s, %s' % (lon, lat))
                reg_id = basic_queries.get_regid(
                    conn, LOGGER, lon, lat)

        except exc.GeoFreshNoResultException as e:
            # TODO: Improve! What I don't like about this: This should not be an exception, but probably
            # quite a normal case...
            LOGGER.debug('Caught this: %s, adding site_id: %s' % (e, site_id))
            if site_id is not None:
                err_msg = '%s (%s)' % (str(e), site_id)
                raise exc.GeoFreshNoResultException(err_msg)
            else:
                raise exc.GeoFreshNoResultException(e)


        ################
        ### Results: ###
        ################

        # Note: This is not GeoJSON (on purpose), as we did not look for geometry yet.
        output_json = {'ids': {}}

        if subc_id is not None:
            output_json['ids']['subc_id'] = subc_id

        if basin_id is not None:
            output_json['ids']['basin_id'] = basin_id

        if reg_id is not None:
            output_json['ids']['reg_id'] = reg_id

        if comment is not None:
            output_json['ids']['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        # In this case, storing a JSON file is totally overdone! But for consistency's sake...
        if utils.return_hyperlink('ids', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('ids', output_json,
                self.metadata, self.job_id,
                self.config['download_dir'],
                self.config['download_url'])
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', output_json

