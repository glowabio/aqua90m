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
curl -X POST https://${PYSERVER}/processes/filter-attribute-by-list/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "items_json_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/outputs-downstream_path-get-shortest-path-to-outlet-plural_shortened.json",
        "keep": {"downstream_segments": [560096607, 560097862, 560099758, 560164915, 560164283, 560168646, 560166133]}
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
        return f'<FilterAttributeByListProcessor> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.debug(f'Start execution: {self.process_id} (job {self.job_id})')
        LOGGER.debug(f'Inputs: {data}')
        LOGGER.log(logging.TRACE, 'Requested outputs: {outputs}')

        try:
            res = self._execute(data, outputs)
            LOGGER.debug(f'Finished execution: {self.process_id} (job {self.job_id})')
            return res

        except Exception as e:
            LOGGER.error(f'During process execution, this happened: {e}')
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?
            #TODO OR: raise ProcessorExecuteError(e, user_msg=e.message)


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
            items_json = utils.download_json(items_json_url)

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
                # This item may be a dict...

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

            input_df = utils.access_csv_as_dataframe(csv_url)

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

        return self.return_results('filtered_data', requested_outputs, output_df=output_df, output_json=output_json, comment=None)





    def return_results(self, resultname, requested_outputs, output_df=None, output_json=None, comment=None):
        # Note: This return_results() is the same as in GeoFreshBaseProcessor, but
        # redefined here, as we don't need all the database functionality that comes
        # with GeoFreshBaseProcessor.

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


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'filter-attribute-by-list'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Filter occurrences by site_id...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "items_json_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/outputs-downstream_path-get-shortest-path-to-outlet-plural_shortened.json",
            "keep": {"downstream_segments": [560096607, 560097862, 560099758, 560164915, 560164283, 560168646, 560166133]},
            "comment": "test1"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

