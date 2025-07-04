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
import pygeoapi.process.aqua90m.utils.exceptions as exc
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
import pygeoapi.process.aqua90m.utils.dataframe_utils as dataframe_utils

'''
# Filter occurrences by site_id: TODO: Missing example data!
curl -i -X POST "http://localhost:5000/processes/filter-attribute-by-list/execution" \
--header "Content-Type: application/json" \
--header "Prefer: respond-async" \
--data '{
  "inputs": {
        "items_json_url": "https://aqua.igb-berlin.de/download/outputs-downstream_path-get-shortest-path-to-outlet-plural-22d6ca92-2600-11f0-ba7f-6fbdd8a35584.json",
        "keep": {"downstream_segments": [561603988, 561707768, 561634372, 561641579, 561659621, 561661100, 561662529, 561665813, 561668697]}
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


class FilterAttributeByListProcessor(BaseProcessor):

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
        return f'<FilterAttributeByListProcessor> {self.name}'


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
            raise ProcessorExecuteError(e) from e # TODO: Can we feed e into ProcessExecuteError?


    def _execute(self, data, requested_outputs):

        ## User inputs:
        # GeoJSON, posted directly / to be downloaded via URL:
        items_json = data.get('items_json', None)
        items_json_url = data.get('items_json_url', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        # Optional comment:
        comment = data.get('comment') # optional
        # Keep which attribute and values?
        keep = data.get('keep')

        ## Check user inputs: TODO: Check all of them.

        if keep is None:
            LOGGER.error("Missing parameter: keep")
            err_msg = "Please provide keep..."
            raise ProcessorExecuteError(err_msg)

        ## Download JSON if user provided URL:
        if items_json_url is not None:
            try:
                LOGGER.debug('Try downloading input JSON from: %s' % items_json_url)
                resp = requests.get(items_json_url)
            except requests.exceptions.SSLError as e:
                LOGGER.warning('SSL error when downloading input data from %s: %s' % (items_json_url, e))
                if ('nimbus.igb-berlin.de' in items_json_url and
                    'nimbus.igb-berlin.de' in str(e) and
                    'certificate verify failed' in str(e)):
                    resp = requests.get(items_json_url, verify=False)

            if not resp.status_code == 200:
                err_msg = 'Failed to download JSON (HTTP %s) from %s.' % (resp.status_code, items_json_url)
                LOGGER.error(err_msg)
                raise exc.DataAccessException(err_msg)
            items_json = resp.json()


        ##################
        ### Actual ... ###
        ##################

        ## Potential outputs:
        output_json = None
        output_df = None

        ## Handle JSON case:
        ## Identify list to iterate over:
        if items_json is not None:
            num_items = None

            if isinstance(items_json, list):
                num_items = len(items_json)
                LOGGER.debug('You passed a list (of length %s)...' % num_items)

            elif not isinstance(items_json, list) and len(items_json) == 1:
                LOGGER.debug('You passed a %s (of length 1), not a list...' % type(items_json))
                key, val = next(iter(items_json.items()))
                if isinstance(val, list):
                    LOGGER.debug('Apparently your list is named "%s". Using that list...' % key)
                    items_json = val
                    num_items = len(items_json)
                    LOGGER.debug('You passed a list (of length %s)...' % num_items)

            else:
                err_msg = 'You passed something we cannot handle (%s), not a list...' % type(items_json)
                LOGGER.error(err_msg)
                raise exc.UserInputException(err_msg)

            ## Now iterate over the list...
            i = 0
            for item in items_json:
                i += 1
                LOGGER.debug('Filtering item %s/%s.' % (i, num_items))

                j = 0
                for keep_attribute in keep.keys():
                    j += 1
                    LOGGER.debug('Filtering attribute %s/%s: "%s"' % (j, len(keep), keep_attribute))

                    num_val = len(item[keep_attribute])
                    LOGGER.debug('Before filtering, "%s" has %s values...' % (keep_attribute, num_val))

                    # Which values should be kept?
                    please_keep = keep[keep_attribute]

                    # Which values are actually kept?
                    kept = []

                    # Keep only the values that should be kept:
                    for value in item[keep_attribute]:
                        if value in please_keep:
                            kept.append(value)

                    # Overwrite the list...
                    item[keep_attribute] = kept
                    num_val = len(item[keep_attribute])
                    item["%s_num_filtered" % keep_attribute] = num_val
                    LOGGER.debug('After filtering, "%s" has %s values...' % (keep_attribute, num_val))

            # Prepare output:
            output_json = {
                "filtered_list": items_json
            }

        ## Handle CSV case:
        elif csv_url is not None:

            err_msg = "Cannot filter CSV yet!"
            LOGGER.error(err_msg)
            raise NotImplementedError(err_msg)

        else:
            err_msg = 'Please provide either JSON (items_json, items_json_url) or CSV data (csv_url).'
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
                    self.download_url)#

                if comment is not None:
                    output_dict_with_url['comment'] = comment

                LOGGER.debug('Outputs: %s' % output_dict_with_url)

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
                output_dict_with_url =  utils.store_to_json_file('filtered_data', output_json,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url

            else:
                return 'application/json', output_json
