import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import pandas as pd
import requests
import tempfile
import urllib
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
import pygeoapi.process.aqua90m.utils.dataframe_utils as dataframe_utils

'''
# Filter occurrences by site_id:
curl -X POST "http://localhost:5000/processes/filter-by-attribute/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://localhost/spdata.csv",
        "keep": {"site_id": [1,10,20]},
        "comment": "schlei-near-rabenholz"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'


# Filter output from get-local-ids:
curl -X POST "https://aqua.igb-berlin.de/pygeoapi-dev/processes/filter-by-attribute/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://localhost/download/outputs-local_ids-get-local-ids-plural-bb55584.csv",
        "keep": {"reg_id": [176]}
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


class FilterByAttributeProcessor(BaseProcessor):

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
        return f'<FilterByAttributeProcessor> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.debug('Start execution: %s (job %s)' % (self.metadata['id'], self.job_id))
        LOGGER.debug('Inputs: %s' % data)
        LOGGER.log(logging.TRACE, 'Requested outputs: %s' % outputs)

        try:
            res = self._execute(data, outputs)
            LOGGER.debug('Finished execution: %s (job %s)' % (self.metadata['id'], self.job_id))
            return res

        except Exception as e:
            LOGGER.error('During process execution, this happened: %s' % e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?


    def _execute(self, data, requested_outputs):

        ## User inputs:
        # GeoJSON, posted directly / to be downloaded via URL:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        #colname_lon = data.get('colname_lon', 'lon')
        #colname_lat = data.get('colname_lat', 'lat')
        #colname_site_id = data.get('colname_site_id', None)
        # Optional comment:
        comment = data.get('comment') # optional
        # Keep which attribute and values?
        keep = data.get('keep')

        ## Check user inputs:
        #if csv_url is not None and colname_site_id is None:
        #    LOGGER.error("Missing parameter: colname_site_id")
        #    err_msg = "Please provide the column name of the site ids inside your csv file (parameter colname_site_id)."
        #    raise ProcessorExecuteError(err_msg)

        if keep is None:
            LOGGER.error("Missing parameter: keep")
            err_msg = "Please provide keep..."
            raise ProcessorExecuteError(err_msg)

        elif keep is not None and len(keep.items()) > 1:
            err_msg = 'Cannot handle more than one keeper yet!'
            LOGGER.error(err_msg)
            raise NotImplementedError(err_msg)


        ## Download GeoJSON if user provided URL:
        if points_geojson_url is not None:
            try:
                LOGGER.debug('Try downloading input GeoJSON from: %s' % points_geojson_url)
                resp = requests.get(points_geojson_url)
            except requests.exceptions.SSLError as e:
                LOGGER.warning('SSL error when downloading input data from %s: %s' % (points_geojson_url, e))
                if ('nimbus.igb-berlin.de' in points_geojson_url and
                    'nimbus.igb-berlin.de' in str(e) and
                    'certificate verify failed' in str(e)):
                    resp = requests.get(points_geojson_url, verify=False)

            if not resp.status_code == 200:
                err_msg = 'Failed to download GeoJSON (HTTP %s) from %s.' % (resp.status_code, points_geojson_url)
                LOGGER.error(err_msg)
                raise exc.DataAccessException(err_msg)
            points_geojson = resp.json()

        ##################
        ### Actual ... ###
        ##################

        ## Potential outputs:
        output_json = None
        output_df = None

        ## Handle GeoJSON case:
        if points_geojson is not None:

            # If a FeatureCollections is passed, check whether the property "site_id" (or similar)
            # is present in every feature:
            #if points_geojson['type'] == 'FeatureCollection':
            #    geojson_helpers.check_feature_collection_property(points_geojson, colname_site_id)

            # Query database:
            #output_json = basic_queries.get_subcid_basinid_regid_for_all_2json(
            #    conn, LOGGER, points_geojson, colname_site_id)
            err_msg = "Cannot filter GeoJSON yet!"
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)


        ## Handle CSV case:
        elif csv_url is not None:
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

            # Filter dataframe
            LOGGER.debug('Input data frame has %s rows.' % input_df.shape[0])
            LOGGER.debug('Input data frame has %s columns: %s' % (input_df.shape[1], input_df.columns))
            i = 0
            for keep_attribute in keep.keys():
                i += 1
                if i > 1:
                    break # currently, only one attribute!
                keep_values = keep[keep_attribute]

                LOGGER.debug('Filtering based on column %s, keeping values %s' % (keep_attribute, keep_values))
                output_df = dataframe_utils.filter_dataframe(input_df, keep_attribute, keep_values)
                LOGGER.debug('Filtering... DONE. Kept %s lines.' % output_df.shape[0])

        else:
            err_msg = 'Please provide either GeoJSON (points_geojson, points_geojson_url) or CSV data (csv_url).'
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)


        #####################
        ### Return result ###
        #####################

        do_return_link = utils.return_hyperlink('filtered_data', requested_outputs)

        ## Return CSV:
        if output_df is not None:
            if do_return_link:
                output_dict_with_url =  utils.store_to_csv_file('filtered_data', output_df,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)

                output_dict_with_url['comment'] = comment

                LOGGER.debug('Outputs: %s' % output_dict_with_url)

                return 'application/json', output_dict_with_url
            else:
                err_msg = 'Not implemented return CSV data directly.'
                LOGGER.error(err_msg)
                raise NotImplementedError(err_msg)

        ## Return JSON:
        elif output_json is not None:
            output_json['comment'] = comment

            if do_return_link:
                output_dict_with_url =  utils.store_to_json_file('filtered_data', output_json,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url

            else:
                return 'application/json', output_json
