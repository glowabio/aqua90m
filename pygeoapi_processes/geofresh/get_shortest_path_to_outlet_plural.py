import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import urllib
import requests
import pandas as pd
import tempfile
import psycopg2
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.utils.exceptions as exc
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request only the ids:
curl -i -X POST "http://localhost:5000/processes/get-shortest-path-to-outlet-plural/execution" \
--header "Content-Type: application/json" \
--header "Prefer: respond-async" \
--data '{
  "inputs": {
        "csv_url": "https://example.igb-berlin.de/download/spdata.csv",
        "colname_lon": "lon",
        "colname_lat": "lat",
        "colname_site_id": "site_id",
        "downstream_ids_only": true,
        "return_csv": true
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class ShortestPathToOutletGetterPlural(BaseProcessor):

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
        return f'<ShortestPathToOutletGetterPlural> {self.name}'


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
        # Option 1:
        # Input:  CSV with lon, lat (or with subc_id)
        # Output: CSV with added columns containing a list of the downstream ids... (TODO: Not ideal as format!)

        # Option 2 (to be implemented, TODO)
        # Input:  GeoJSON with points
        # Output: GeoJSON with points, and for each point, a list of the downstream ids

        # User inputs
        return_csv  = data.get('return_csv',  False)
        return_json = data.get('return_json', False)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', 'lon')
        colname_lat = data.get('colname_lat', 'lat')
        colname_site_id = data.get('colname_site_id', None)
        comment = data.get('comment', None)
        geometry_only = data.get('geometry_only', False)
        downstream_ids_only = data.get('downstream_ids_only', False)
        add_downstream_ids = data.get('add_downstream_ids', True)
        # GeoJSON:
        points_geojson = None # TODO

        # Check parameters:
        if csv_url is not None:
            if colname_site_id is None:
                err_msg = "If you provide a CSV file, you must provide colname_site_id!"
                LOGGER.error(err_msg)
                raise ProcessorExecuteError(err_msg)
            if not downstream_ids_only:
                err_msg = "Cannot return geometries for CSV input yet! (And probably never will, because returning geometry inside a CSV does not make sense...)"
                LOGGER.error(err_msg)
                raise NotImplementedError(err_msg)
                # TODO: Any idea how to return linestrings in a csv? Is that required, or even desired at all?

        if return_csv and return_json:
            err_msg = "Please set either return_csv or return_json to true (not both)!"
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)
        if not return_csv and not return_json:
            err_msg = "Please set either return_csv or return_json to true!"
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)


        ##################
        ### Actual ... ###
        ##################
        # Overall goal: Get the dijkstra shortest path (as linestrings)!

        ## Potential outputs:
        output_json = None
        output_df = None

        ## Handle GeoJSON case:
        if points_geojson is not None:

            err_msg = "Cannot return downstream paths for GeoJSON input yet! (Let us know if you would like this functionality)."
            LOGGER.error(err_msg)
            raise NotImplementedError(err_msg)

            # If a FeatureCollections is passed, check whether the property "site_id" (or similar)
            # is present in every feature:
            if points_geojson['type'] == 'FeatureCollection':
                geojson_helpers.check_feature_collection_property(points_geojson, colname_site_id)

        ## Handle CSV case:
        ## TODO: Bring download csv to utils!
        elif csv_url is not None:

            # Download CSV:
            LOGGER.debug('Accessing input CSV from: %s' % csv_url)
            try:
                input_df = pd.read_csv(csv_url) # tries comma

                if input_df.shape[1] == 1:
                    LOGGER.debug('Found only one column (name "%s"). Maybe it is not comma-separeted, but semicolo-separated? Trying...' % input_df.columns)
                    input_df = pd.read_csv(csv_url, sep=';')

                LOGGER.debug('Accessing input CSV... DONE. Found %s columns (names: %s)' % (input_df.shape[1], input_df.columns))

            # Files stored on Nimbus: We get SSL error:
            except urllib.error.URLError as e:
                LOGGER.warning('SSL error when downloading input CSV from %s: %s' % (csv_url, e))
                if ('nimbus.igb-berlin.de' in csv_url and
                    'certificate verify failed' in str(e)):
                    LOGGER.debug('Will download input CSV with verify=False to a tempfile.')
                    resp = requests.get(csv_url, verify=False)
                    if resp.status_code == 200:
                        mytempfile = tempfile.NamedTemporaryFile()
                        mytempfile.write(resp.content)
                        mytempfile.flush()
                        mytempfilename = mytempfile.name
                        LOGGER.debug("CSV file stored to tempfile successfully: %s" % mytempfilename)
                        input_df = pd.read_csv(mytempfilename)
                        mytempfile.close()
                    else:
                        err_msg = 'Could not download CSV input data from %s (HTTP %s)' % (csv_url, resp.status_code)
                        LOGGER.error(err_msg)
                        raise exc.DataAccessException(err_msg)

            ## Now, for each row, get the ids (unless already present)!
            if 'subc_id' in input_df.columns:
                LOGGER.debug('Input dataframe already contains subc_id for each point, using that...')
                temp_df = input_df
            else:
                LOGGER.debug('Querying subc_id etc. for each point...')
                temp_df = basic_queries.get_subcid_basinid_regid_for_all_1csv(
                    conn, LOGGER, input_df, colname_lon, colname_lat, colname_site_id)

            ## Next, for each row, get the downstream ids!
            if return_csv:
                output_df = routing.get_dijkstra_ids_to_outlet_one_loop(conn, temp_df, colname_site_id, return_csv=True)
            elif return_json:
                output_json = routing.get_dijkstra_ids_to_outlet_one_loop(conn, temp_df, colname_site_id, return_json=True)


        #####################
        ### Return result ###
        #####################

        do_return_link = utils.return_hyperlink('downstream_path', requested_outputs)

        ## Return CSV:
        if output_df is not None:
            if do_return_link:
                output_dict_with_url =  utils.store_to_csv_file('downstream_path', output_df,
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
                output_dict_with_url =  utils.store_to_json_file('downstream_path', output_json,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)

                return 'application/json', output_dict_with_url

            else:
                return 'application/json', output_json
