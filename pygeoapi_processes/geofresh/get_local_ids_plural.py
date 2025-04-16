import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import pandas as pd
import psycopg2
import requests
import tempfile
import urllib
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
import pygeoapi.process.aqua90m.utils.exceptions as exc
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

# Request with CSV input and CSV output:
curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
# NOT COMMIT:
curl -X POST --location 'https://aqua.igb-berlin.de/pygeoapi-dev/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "csv_url": "https://nimbus.igb-berlin.de/index.php/s/SnDSamy56sLWs2s/download/spdata.csv",
        "colname_lat": "latitude",
        "colname_lon": "longitude",
        "colname_site_id": "site_id",
        "comment": "schlei-near-rabenholz"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'


# Request plain JSON (not GeoJSON: Cannot request Feature/Geometry, does not apply)
# Input points: GeoJSON
curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
# NOT COMMIT:
curl -X POST --location 'https://aqua.igb-berlin.de/pygeoapi-dev/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "points_geojson": {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"site_id": 1},
                    "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
                },
                {
                    "type": "Feature",
                    "properties": {"site_id": 2},
                    "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
                },
                {
                    "type": "Feature",
                    "properties": {"site_id": 3},
                    "geometry": { "coordinates": [ 11.915323076217902, 52.730867141970464 ], "type": "Point" }
                },
                {
                    "type": "Feature",
                    "properties": {"site_id": 4},
                    "geometry": { "coordinates": [ 16.651903948708565, 48.27779486850176 ], "type": "Point" }
                },
                {
                    "type": "Feature",
                    "properties": {"site_id": 5},
                    "geometry": { "coordinates": [ 19.201146608148463, 47.12192880511424 ], "type": "Point" }
                },
                {
                    "type": "Feature",
                    "properties": {"site_id": 6},
                    "geometry": { "coordinates": [ 24.432498016999062, 61.215505889934434 ], "type": "Point" }
                }
            ]
        },
        "comment": "schlei-near-rabenholz"
    }
}'


curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
# NOT COMMIT:
curl -X POST --location 'https://aqua.igb-berlin.de/pygeoapi-dev/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "points_geojson": {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"site_id": 1},
                    "geometry": { "coordinates": [ 10.041155219078064, 53.07006147583069 ], "type": "Point" }
                },
                {
                    "type": "Feature",
                    "properties": {"site_id": 2},
                    "geometry": { "coordinates": [ 10.042726993560791, 53.06911450500803 ], "type": "Point" }
                },
                {
                    "type": "Feature",
                    "properties": {"site_id": 3},
                    "geometry": { "coordinates": [ 10.039894580841064, 53.06869677412868 ], "type": "Point" }
                }
            ]
        },
        "comment": "schlei-near-rabenholz"
    }
}'


# Request plain JSON (not GeoJSON: Cannot request Feature/Geometry, does not apply)
# Input points: Lonlat string
curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
# NOT COMMIT:
curl -X POST --location 'https://aqua.igb-berlin.de/pygeoapi-dev/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "lonlatstring": "10.698832912677716,53.51710727672125;12.80898022975407,52.42187129944509;11.915323076217902,52.730867141970464;16.651903948708565,48.27779486850176;19.201146608148463,47.12192880511424;24.432498016999062,61.215505889934434",
        "comment": "schlei-near-rabenholz"
    }
}'

'''

'''
Example result:
# TODO: Discuss if this result is understandable enough!
{
    "subc_ids":"507307015, 90929627, 553374842, 507081236, 506601172, 91875954",
    "region_ids":"58, 65, 159",
    "basin_ids":"1291835, 1291763, 1294020",
    "everything":{
        "58":{
            "1294020":{
                "506601172":[
                    {
                        "coordinates":[
                            10.698832912677716,
                            53.51710727672125
                        ],
                        "type":"Point"
                    }
                ],
                "507307015":[
                    {
                        "coordinates":[
                            12.80898022975407,
                            52.42187129944509
                        ],
                        "type":"Point"
                    }
                ],
                "507081236":[
                    {
                        "coordinates":[
                            11.915323076217902,
                            52.730867141970464
                        ],
                        "type":"Point"
                    }
                ]
            }
        },
        "159":{
            "1291835":{
                "90929627":[
                    {
                        "coordinates":[
                            16.651903948708565,
                            48.27779486850176
                        ],
                        "type":"Point"
                    }
                ],
                "91875954":[
                    {
                        "coordinates":[
                            19.201146608148463,
                            47.12192880511424
                        ],
                        "type":"Point"
                    }
                ]
            }
        },
        "65":{
            "1291763":{
                "553374842":[
                    {
                        "coordinates":[
                            24.432498016999062,
                            61.215505889934434
                        ],
                        "type":"Point"
                    }
                ]
            }
        }
    },
    "comment": "schlei-bei-rabenholz"
}

TODO: Maybe rather:


{
    "subc_ids":"507307015, 90929627, 553374842, 507081236, 506601172, 91875954",
    "region_ids":"58, 65, 159",
    "basin_ids":"1291835, 1291763, 1294020",
    "everything":{
        "regional_unit_ids": {
            "58": {
                "basin_ids": {
                    "1294020": {
                        "subcatchment_ids": {
                            "506601172": [...point, point, point...],
                            "50660117x": [...point, point, point...],
                        }
                    }
                }
            }
        }
    }
        

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalIdGetterPlural(BaseProcessor):

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
        return f'<LocalIdGetterPlural> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.debug('Start execution: %s (job %s)' % (self.metadata['id'], self.job_id))
        LOGGER.debug('Inputs: %s' % data)
        LOGGER.log(logging.TRACE, 'Requested outputs: %s' % outputs)

        try:
            conn = get_connection_object_config(self.config)
            res = self._execute(data, outputs, conn)
            LOGGER.debug('Finished execution: %s (job %s)' % (self.metadata['id'], self.job_id))
            LOGGER.log(5, 'Closing connection...')
            conn.close()
            LOGGER.log(5, 'Closing connection... Done.')
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

        ## User inputs:
        # GeoJSON, posted directly / to be downloaded via URL:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', None)
        colname_lat = data.get('colname_lat', None)
        colname_site_id = data.get('colname_site_id', None)
        colname_subc_id = data.get('colname_subc_id', None) # special case, optional
        # Optional comment:
        comment = data.get('comment') # optional
        # Which ids are requested:
        which_ids = data.get('which_ids', 'subc_id, basin_id, reg_id')
        which_ids = which_ids.replace(' ', '')
        which_ids = which_ids.split(',')

        ## Check user inputs:

        if csv_url is not None and colname_site_id is None:
            LOGGER.error("Missing parameter: colname_site_id")
            err_msg = "Please provide the column name of the site ids inside your csv file (parameter colname_site_id)."
            raise ProcessorExecuteError(err_msg)

        possible_ids = ['subc_id', 'basin_id', 'reg_id']
        if not all([some_id in possible_ids for some_id in which_ids]):
            err_msg = "The requested ids have to be one or several of: %s (you provided %s)" % (possible_ids, which_ids)
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)


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
            if points_geojson['type'] == 'FeatureCollection':
                geojson_helpers.check_feature_collection_property(points_geojson, colname_site_id)

            # Query database:
            if 'subc_id' in which_ids:
                output_json = basic_queries.get_subcid_basinid_regid_for_all_1(
                    conn, LOGGER, points_geojson, colname_site_id)
            elif 'basin_id' in which_ids:
                err_msg = "Currently, for GeoJSON input, only all ids can be returned."
                raise NotImplementedError(err_msg) # TODO
            elif 'reg_id' in which_ids:
                err_msg = "Currently, for GeoJSON input, only all ids can be returned."
                raise NotImplementedError(err_msg) # TODO
            # Note: The case where users input subc_ids and want basin_id and reg_id cannot be
            # handled in GeoJSON, as GeoJSON must by definition contain coordinates!
            # In this case, instead of GeoJSON, we'd have to let them input just some dictionary.
            # Probably not needed ever, so why implement that. It would just be for completeness.
            # Implement when needed.



        ## Handle CSV case:
        elif csv_url is not None:
            LOGGER.debug('Accessing input CSV from: %s' % csv_url)
            try:
                input_df = pd.read_csv(csv_url)
                LOGGER.debug('Accessing input CSV... Done.')

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

            # Query database:
            if colname_subc_id is not None:
                # Special case! User provided CSV with a column containing subc_ids...
                output_df = basic_queries.get_basinid_regid_for_all_from_subcid_2(
                    conn, LOGGER, input_df, colname_subc_id, colname_site_id)

            elif 'subc_id' in which_ids:
                output_df = basic_queries.get_subcid_basinid_regid_for_all_2(
                    conn, LOGGER, input_df, colname_lon, colname_lat, colname_site_id)
            elif 'basin_id' in which_ids:
                output_df = basic_queries.get_basinid_regid_for_all_2(
                    conn, LOGGER, input_df, colname_lon, colname_lat, colname_site_id)
            elif 'reg_id' in which_ids:
                output_df = basic_queries.get_regid_for_all_2(
                    conn, LOGGER, input_df, colname_lon, colname_lat, colname_site_id)

        else:
            err_msg = 'Please provide either GeoJSON (points_geojson, points_geojson_url) or CSV data (csv_url).'
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)


        #####################
        ### Return result ###
        #####################

        do_return_link = utils.return_hyperlink('local_ids', requested_outputs)

        ## Return CSV:
        if output_df is not None:
            if do_return_link:
                output_dict_with_url =  utils.store_to_csv_file('local_ids', output_df,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)

                output_dict_with_url['comment'] = comment

                return 'application/json', output_dict_with_url
            else:
                err_msg = 'Not implemented return CSV data directly.'
                LOGGER.error(err_msg)
                raise NotImplementedError(err_msg)

        ## Return JSON:
        elif output_json is not None:
            output_json['comment'] = comment

            if do_return_link:
                output_dict_with_url =  utils.store_to_json_file('local_ids', output_json,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url

            else:
                return 'application/json', output_json
