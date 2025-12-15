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
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers

'''
# Filter occurrences by site_id:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "keep": {"site_id": ["FP1", "FP10", "FP20"]},
        "comment": "barbus sites"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'


# Filter occurrences by site_id and latitude:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "keep": {"site_id": ["FP1", "FP10", "FP20"], "latitude": [40.299111]},
        "comment": "barbus sites"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Filter occurrences by site_id and latitude:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "keep": {"site_id": ["FP1", "FP10", "FP20"]},
        "conditions": {"longitude": "x<20.8"},
        "comment": "barbus sites"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Filtering by species name: TODO: Missing example data!
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/xyz",
        "keep": {"species": ["Salaria fluviatilis", "Squalius peloponensis"]},
        "comment": "species list"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Filter output from get-local-ids: TODO: Missing example data!
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/download/outputs-local_ids-get-local-ids-plural-bb5be376-1adc-11f0-ba7f-6fbdd8a35584.csv",
        "keep": {"reg_id": [176]}
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Filtering by condition, using GeoJSON input:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--data '{
  "inputs": {
        "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points2.json",
        "conditions": {"temperature": ">=30"},
        "comment": "temperature"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Filtering by value, using GeoJSON input:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--data '{
  "inputs": {
        "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points2.json",
        "keep": {"site_id": [1, 5, 6]},
        "comment": "filter site ids"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'


# Filtering by value, using GeoJSON input (directly):
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--data '{
  "inputs": {
        "points_geojson":
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"site_id": 1, "temperature": 20},
                        "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
                    },
                    {
                        "type": "Feature",
                        "properties": {"site_id": 2, "temperature": 30},
                        "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
                    }
                ]
            },
        "keep": {"site_id": [1, 5, 6]},
        "comment": "filter site ids"
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
            raise ProcessorExecuteError(e) from e # TODO: Can we feed e into ProcessExecuteError?


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
        keep = data.get('keep', None)
        conditions = data.get('conditions', None)
        # TODO: With the dictionary format, users cannot pass several conditions for one attribute, e.g. x>10 and x>5...

        ## Check user inputs:
        #if csv_url is not None and colname_site_id is None:
        #    LOGGER.error("Missing parameter: colname_site_id")
        #    err_msg = "Please provide the column name of the site ids inside your csv file (parameter colname_site_id)."
        #    raise ProcessorExecuteError(err_msg)

        # Error if missing...
        utils.at_least_one_param(dict(
            keep=keep,
            conditions=conditions))
        utils.exactly_one_param(dict(
            points_geojson=points_geojson,
            points_geojson_url=points_geojson_url,
            csv_url=csv_url))

        ## Download if user provided URL:
        input_df = None
        if points_geojson_url is not None:
            points_geojson = utils.download_geojson(points_geojson_url)
        elif csv_url is not None:
            input_df = utils.access_csv_as_dataframe(csv_url)


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
            if points_geojson['type'] == 'FeatureCollection':
                pass
                #geojson_helpers.check_feature_collection_property(points_geojson, colname_site_id)
            else:
                err_msg = "Need a FeatureCollection to be able to filter."

            # Filter geojson by value-list, iteratively:
            if keep is not None:
                for keep_attribute, keep_values in keep.items():
                    LOGGER.debug(f'Filtering based on property {keep_attribute}, keeping values {keep_values}')
                    geojson_helpers.check_feature_collection_property(points_geojson, keep_attribute)
                    points_geojson = geojson_helpers.filter_geojson(points_geojson, keep_attribute, keep_values)
                    LOGGER.debug(f'Filtering based on property {keep_attribute}: kept {len(points_geojson["features"])} features.')
                LOGGER.debug(f'Filtering... DONE. Kept {len(points_geojson["features"])} features.')
                output_json = points_geojson


            # Filter geojson by numeric condition, iteratively:
            if conditions is not None:
                for keep_attribute, condition in conditions.items():
                    LOGGER.debug(f'Filtering based on property {keep_attribute}, keeping values {condition}')
                    geojson_helpers.check_feature_collection_property(points_geojson, keep_attribute)
                    condition_dict = dataframe_utils.parse_filter_condition(condition, var="x")
                    LOGGER.debug(f'FILTER CONDITION: {condition_dict}')
                    points_geojson = geojson_helpers.filter_geojson_by_condition(
                        points_geojson, keep_attribute, condition_dict)
                    LOGGER.debug(f'Filtering based on property {keep_attribute}: kept {len(points_geojson["features"])} features.')
                LOGGER.debug(f'Filtering... DONE. Kept {len(points_geojson["features"])} features.')
                output_json = points_geojson


        ## Handle CSV case:
        elif input_df is not None:
            LOGGER.debug(f'Input data frame has {input_df.shape[1]} columns: {input_df.columns}.')
            LOGGER.debug(f'Input data frame has {input_df.shape[0]} rows.')

            # Filter dataframe by value-list, iteratively:
            if keep is not None:
                for keep_attribute in keep.keys():
                    keep_values = keep[keep_attribute]
                    LOGGER.debug(f'Filtering based on column {keep_attribute}, keeping values {keep_values}')
                    input_df = dataframe_utils.filter_dataframe(input_df, keep_attribute, keep_values)
                    LOGGER.debug(f'Filtering based on column {keep_attribute} kept {input_df.shape[0]} rows.')
                LOGGER.debug(f'Filtering... DONE. Kept {input_df.shape[0]} rows.')
                output_df = input_df

            # Filter dataframe by numeric condition, iteratively:
            if conditions is not None:
                for keep_attribute, condition in conditions.items():
                    LOGGER.debug(f'Filtering based on column {keep_attribute}, keeping values {condition}')
                    condition_dict = dataframe_utils.parse_filter_condition(condition, var="x")
                    input_df = dataframe_utils.filter_dataframe_by_condition(
                        input_df, keep_attribute, condition_dict)
                    LOGGER.debug(f'Filtering based on column {keep_attribute}: kept {input_df.shape[0]} rows.')
                LOGGER.debug(f'Filtering... DONE. Kept {input_df.shape[0]} rows.')
                output_df = input_df


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
