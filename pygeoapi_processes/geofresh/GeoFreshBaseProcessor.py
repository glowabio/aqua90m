import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import traceback
import json
import psycopg2
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


class GeoFreshBaseProcessor(BaseProcessor):

    def __init__(self, processor_def: dict, process_metadata: dict):
        super().__init__(processor_def, process_metadata)
        self.supports_outputs = True
        self.process_id = self.metadata["id"]
        self.job_id = None
        self.config = None
        self.download_dir = None
        self.download_url = None

        # Set config:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            self.config = json.load(config_file)
            self.download_dir = self.config['download_dir']
            self.download_url = self.config['download_url']


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<GeoFreshBaseProcessor> {self.process_id}'


    def execute(self, data, outputs=None):
        LOGGER.debug(f'Start execution: {self.process_id} (job {self.job_id})')
        LOGGER.debug(f'Inputs: {data}')
        LOGGER.log(logging.TRACE, 'Requested outputs: {outputs}')

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
            error_message = f'Database error: {err} ({str(e3)})'
            LOGGER.error(error_message)
            raise ProcessorExecuteError(user_msg = error_message)

        except Exception as e:
            conn.close()
            LOGGER.error(f'During process execution, this happened: {e}')
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?
            #TODO OR: raise ProcessorExecuteError(e, user_msg=e.message)


    def _execute(self, data, requested_outputs, conn):
        LOGGER.error('To be implemented by derived classes...')
        pass




    def return_results(self, resultname, requested_outputs, output_df=None, output_json=None, comment=None):

        do_return_link = utils.return_hyperlink(resultname, requested_outputs)

        ## Return CSV:
        if output_df is not None:
            if do_return_link:
                output_dict_with_url =  utils.store_to_csv_file(resultname, output_df,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)

                if comment is not None:
                    output_dict_with_url['comment'] = comment

                return 'application/json', output_dict_with_url
            else:
                err_msg = 'Not implemented return CSV data directly.'
                LOGGER.error(err_msg)
                raise NotImplementedError(err_msg)

        ## Return JSON:
        elif output_json is not None:

            if comment is not None:
                output_json['comment'] = comment

            if do_return_link:
                output_dict_with_url =  utils.store_to_json_file(resultname, output_json,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url

            else:
                return 'application/json', output_json



