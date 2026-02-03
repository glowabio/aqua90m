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
# for updating process status, only for TinyDB manager...
from pygeoapi.util import JobStatus as JobStatus
from pygeoapi.config import get_config as get_config
import tinydb
from filelock import FileLock

class GeoFreshBaseProcessor(BaseProcessor):

    def __init__(self, processor_def: dict, process_metadata: dict):
        super().__init__(processor_def, process_metadata)
        self.supports_outputs = True
        self.process_id = self.metadata["id"]
        self.job_id = None
        self.config = None
        self.download_dir = None
        self.download_url = None
        self.tinydb_job_status_file = None

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

    def update_status(self, msg, progress=None):
        # Note: Use sparsely, this is expensive.

        # First time, read config to find the TinyDB file to be updated...
        if self.tinydb_job_status_file is None:
            LOGGER.debug('Updating the process status (first time)...')

            LOGGER.debug(f'Loading config from: {os.environ.get('PYGEOAPI_CONFIG')}...')
            pygeo_config = get_config()
            manager_name = pygeo_config['server']['manager']['name']
            LOGGER.debug(f'Config contains a job manager of type {manager_name}.')
            if manager_name == 'TinyDB':
                self.tinydb_job_status_file = pygeo_config['server']['manager']['connection']
            else:
                self.tinydb_job_status_file = False
                LOGGER.warn(f"Cannot update job status ever on this instance, manager is not TinyDB, but {manager_name}")

        # Cannot update if not TinyDB:
        elif self.tinydb_job_status_file == False:
            LOGGER.debug(f"Not updating job status because manager is not TinyDB.")
            return

        LOGGER.debug(f'Updating the process status. TinyDB file: {self.tinydb_job_status_file}')

        status_dict = {
            "status": JobStatus.running.value,
            "message": msg
        }
        if progress is not None:
            status_dict['progress'] = progress

        with FileLock(f"{self.tinydb_job_status_file}.lock"):
            mydb = tinydb.TinyDB(self.tinydb_job_status_file)
            mydb.update(status_dict, tinydb.where('identifier') == self.job_id)
            mydb.close()

        LOGGER.debug('Updating the process status... done.')


    def execute(self, data, outputs=None):
        LOGGER.debug(f'Start execution: {self.process_id} (job {self.job_id}, os pid {os.getpid()})')
        LOGGER.debug(f'Inputs: {data}')
        LOGGER.log(logging.TRACE, 'Requested outputs: {outputs}')
        conn = None # Needed in case exceptino is raised during get_connection_object_config()

        try:
            conn = get_connection_object_config(self.config)
            self.update_status('Started execution', 6)
            res = self._execute(data, outputs, conn)
            LOGGER.debug(f'Finished execution: {self.process_id} (job {self.job_id})')
            LOGGER.log(logging.TRACE, 'Closing connection...')
            conn.close()
            LOGGER.log(logging.TRACE, 'Closing connection... Done.')
            return res

        except psycopg2.Error as e3:
            # Prepare error messages for user (understandable) and for log (detailed)
            err_msg_log = 'no message.'

            # Make user-safe messages from various database exceptions:
            if isinstance(e3, psycopg2.OperationalError):
                err_msg_user = "Temporarily unavailable (details hidden, please ask admin/see log)"
                # Examples:
                # psycopg2:OperationalError: connection to server at "172.16.4.76", port 5432 failed: Connection refused
                # psycopg2:OperationalError: SSL SYSCALL error: EOF detected (SSL SYSCALL error: EOF detected
                # psycopg2:OperationalError: could not translate host name "172.16.4.7666" to address: Name or service not known
                # psycopg2:OperationalError: invalid port number: "543222"
                # psycopg2:OperationalError: connection to server at "172.16.4.76", port 5432 failed: FATAL:  no pg_hba.conf entry for host "172.16.8.145", user "shiny_user", database "geofreshihh_data", SSL encryption
                # psycopg2:OperationalError: connection to server at "172.16.4.76", port 5432 failed: FATAL:  no pg_hba.conf entry for host "172.16.8.145", user "shinyy_user", database "geofresh_data", SSL encryption
                # psycopg2:OperationalError: connection to server at "172.16.4.76", port 5432 failed: FATAL:  password authentication failed for user "shiny_user"
            elif isinstance(e3, psycopg2.errors.QueryCanceled):
                err_msg_user = "Request timed out (details hidden, please ask admin/see log)"
            elif isinstance(e3, psycopg2.IntegrityError):
                err_msg_user = "Resource conflict (details hidden, please ask admin/see log)"
            elif isinstance(e3, psycopg2.DataError):
                err_msg_user = "Invalid input (details hidden, please ask admin/see log)"
            else:
                # For example:
                #psycopg2.InterfaceError
                #psycopg2.InternalError
                #psycopg2.ProgrammingError
                #psycopg2.errors.SerializationFailure
                #psycopg2.errors.SerializationFailure
                #psycopg2.errors.InsufficientPrivilege
                err_msg_user = 'Database error (details hidden, please ask admin/see log).'

            # Collect details about error for log:
            module_name = type(e3).__module__.removesuffix('.errors')
            exception_name = type(e3).__name__
            exception_message = str(e3).rstrip()
            exception_full = f"{module_name}:{exception_name}: {exception_message}"
            # Example: psycopg2:OperationalError: connection to server at "172.16.4.76",
            # port 5432 failed: Connection refused. Is the server running on that host
            # and accepting TCP/IP connections?

            # Check if connection was opened and has to be closed, or never opened:
            if conn is not None:
                err_msg_log = f"Initial connection to GeoFRESH database successful, but: {exception_full}"
                LOGGER.debug('Now closing connection (as part of error handling)')
                conn.close()
                LOGGER.debug('Database connection was closed by us (as part of error handling).')
            else:
                err_msg_user = f"Was not able to connect to the database: {err_msg_user}"
                err_msg_log = f"Not able to connect to GeoFRESH database: {exception_full}"

            # Log and raise:
            LOGGER.error(err_msg_log)
            err_msg_user = f"Database error: {err_msg_user}"
            raise ProcessorExecuteError(user_msg = err_msg_user)

        except Exception as e:
            if conn is not None:
                conn.close()
            LOGGER.error(f'During process execution, this happened: {repr(e)}')
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



