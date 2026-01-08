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
import pygeoapi.process.aqua90m.utils.conversion as conversion

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

# Filtering by species name:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/species_occurrences_cuba_maxine.csv",
        "keep": {"species": ["Sagittaria_lancifolia", "Salvinia_auriculata"]},
        "comment": "species list"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Filter output from get-local-ids:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/outputs-local_ids-get-local-ids-plural.csv",
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


# Filter occurrences by site_id:
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "keep": {"site_id": ["FP1", "FP10", "FP20"]},
        "comment": "barbus sites",
        "result_format": "geojson",
        "colname_lat": "latitude",
        "colname_lon": "longitude"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Filter occurrences by site_id, but error due to omitting colnames:
# Works (tested 2025-12-15)
curl -X POST https://${PYSERVER}/processes/filter-by-attribute/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "keep": {"site_id": ["FP1", "FP10", "FP20"]},
        "comment": "barbus sites",
        "result_format": "geojson"
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
        return f'<FilterByAttributeProcessor> {self.name}'


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
        result_format = data.get('result_format', None)
        # GeoJSON, posted directly / to be downloaded via URL:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', None)
        colname_lat = data.get('colname_lat', None)
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

            # Format of the result defaults to the input format:
            if result_format is None:
                result_format = "geojson"

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

            # Format of the result (defaults to the input format):
            if result_format is None:
                result_format = "csv"
            elif result_format == "geojson":
                msg = " If your input is CSV and you want GeoJSON output, specifying the names of the lon and lat column is mandatory."
                utils.mandatory_parameters(
                    dict(colname_lat=colname_lat, colname_lon=colname_lon),
                    additional_message=msg)

            # Filter dataframe by value-list, iteratively:
            if keep is not None:
                for keep_attribute, keep_values in keep.items():
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

        ## Convert result to other format, if explicitly requested:
        if result_format == "csv" and output_df is None:
            LOGGER.debug('User requested converting output to csv...')
            # If the user specified column names, use those:
            colname_lon = colname_lon or "lon"
            colname_lat = colname_lat or "lat"
            # Convert:
            output_df = conversion.geojson_points_to_dataframe(
                output_json, colname_lon="lon", colname_lat="lat")
        elif result_format == 'geojson' and output_json is None:
            LOGGER.debug('User requested converting output to geojson...')
            output_json = conversion.dataframe_to_geojson_points(
                output_df, colname_lon, colname_lat)

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
    process_id = 'filter-by-attribute'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Filter occurrences by site_id...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "keep": {"site_id": ["FP1", "FP10", "FP20"]},
            "comment": "test1"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 2: Filter occurrences by site_id and latitude (equality)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "keep": {"site_id": ["FP1", "FP10", "FP20"], "latitude": [40.299111]},
            "comment": "test2"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 3: Filter occurrences by site_id (equality) and latitude (smaller-than)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "keep": {"site_id": ["FP1", "FP10", "FP20"]},
            "conditions": {"longitude": "x<20.8"},
            "comment": "test3"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 4: Filter occurrences by species name (equality)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/species_occurrences_cuba_maxine.csv",
            "keep": {"species": ["Sagittaria_lancifolia", "Salvinia_auriculata"]},
            "comment": "test4"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 5: Filter occurrences by temperature (smaller-than-equals), GeoJSON file input...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points2.json",
            "conditions": {"temperature": ">=30"},
            "comment": "test5"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 6: Filter occurrences by site_id (equality), GeoJSON input directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
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
            "comment": "test6"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 7: Filter occurrences by site_id...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "keep": {"site_id": ["FP1", "FP10", "FP20"]},
            "result_format": "geojson",
            "colname_lat": "latitude",
            "colname_lon": "longitude",
            "comment": "test7"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 8: Will fail: Missing input params...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "keep": {"site_id": ["FP1", "FP10", "FP20"]},
            "result_format": "geojson",
            "comment": "test8"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        print(f'TEST CASE 8: EXPECTED: {e.response.json()["description"]}')


    print('TEST CASE 9: Filter output of get-local-ids-plural by reg_id...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/outputs-local_ids-get-local-ids-plural.csv",
            "keep": {"reg_id": [9999]},
            "comment": "test9"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

