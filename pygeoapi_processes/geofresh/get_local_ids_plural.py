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
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

## Requesting only reg_id. CSV input and output.
## INPUT:  CSV File
## OUTPUT: CSV File
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-ids-plural/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "colname_lat": "latitude",
        "colname_lon": "longitude",
        "colname_site_id": "site_id",
        "which_ids": "reg_id",
        "comment": "schlei-near-rabenholz"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

## INPUT:  GeoJSON File (FeatureCollection)
## OUTPUT: Plain JSON File
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-ids-plural/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "colname_site_id": "my_site",
        "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
        "comment": "schlei-near-rabenholz"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

## INPUT:  GeoJSON File (GeometryCollection)
## OUTPUT: Plain JSON File
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-ids-plural/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "colname_site_id": "my_site",
        "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometrycollection_points.json",
        "comment": "schlei-near-rabenholz"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

## INPUT:  GeoJSON directly (FeatureCollection)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-ids-plural/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "colname_site_id": "site_id",
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

## INPUT:  GeoJSON directly (GeometryCollection)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-ids-plural/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "points_geojson": {
            "type": "GeometryCollection",
            "geometries": [
                {
                    "type": "Point",
                    "coordinates": [20.087421, 39.364848]
                },
                {
                    "type": "Point",
                    "coordinates": [27.846357, 36.548812]
                },
                {
                    "type": "Point",
                    "coordinates": [25.73764, 35.24806]
                },
                {
                    "type": "Point",
                    "coordinates": [24.17569, 35.50542]
                }
            ]
        },
        "which_ids": ["subc_id", "basin_id", "reg_id"],
        "comment": "schlei-near-rabenholz"
    }
}'

## TODO: Not Implemented: Requesting only reg_id when input is GeoJSON
## INPUT:  GeoJSON directly (GeometryCollection)
## OUTPUT: Plain JSON directly
curl -X POST https://${PYSERVER}/processes/get-local-ids-plural/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "points_geojson": {
            "type": "GeometryCollection",
            "geometries": [
                {
                    "type": "Point",
                    "coordinates": [20.087421, 39.364848]
                },
                {
                    "type": "Point",
                    "coordinates": [27.846357, 36.548812]
                },
                {
                    "type": "Point",
                    "coordinates": [25.73764, 35.24806]
                },
                {
                    "type": "Point",
                    "coordinates": [24.17569, 35.50542]
                }
            ]
        },
        "which_ids": "reg_id",
        "comment": "schlei-near-rabenholz"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

## INPUT:  GeoJSON directly (FeatureCollection)
## OUTPUT: Plain JSON File
## Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-local-ids-plural/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "colname_site_id": "site_id",
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


class LocalIdGetterPlural(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        ####################
        ### User inputs: ###
        ####################

        # GeoJSON, posted directly / to be downloaded via URL:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', None)
        colname_lat = data.get('colname_lat', None)
        colname_site_id = data.get('colname_site_id', None)
        colname_subc_id = data.get('colname_subc_id', None) # special case, optional
        # Which ids are requested:
        which_ids = data.get('which_ids', ['subc_id', 'basin_id', 'reg_id'])
        result_format = data.get('result_format', None)
        # Optional comment:
        comment = data.get('comment') # optional

        ##############################
        ### Download if applicable ###
        ### Validate GeoJSON       ###
        ##############################

        if points_geojson_url is not None:
            points_geojson = utils.download_geojson(points_geojson_url)

        input_df = None
        if csv_url is not None:
            input_df = utils.access_csv_as_dataframe(csv_url)

        if points_geojson is not None:
            # If a FeatureCollections is passed, check whether the property "site_id" (or similar)
            # is present in every feature:
            if points_geojson['type'] == 'FeatureCollection':
                geojson_helpers.check_feature_collection_property(points_geojson, colname_site_id)

        # Set result_format to input format:
        if result_format is None:
            if points_geojson is not None:
                result_format = 'geojson'
            elif csv_url is not None:
                result_format = 'csv'


        #######################
        ### Validate inputs ###
        #######################

        # Check/adapt data type of which_ids param:
        if not isinstance(which_ids, list) and isinstance(which_ids, str):
            # If user did not put the word into a list...
            which_ids = [which_ids]

        # Provide at least one type of input data:
        utils.at_least_one_param(dict(
            points_geojson=points_geojson,
            points_geojson_url=points_geojson_url,
            csv_url=csv_url
        ))

        # In CSV case, check mandatory columns
        if input_df is not None:
            if colname_subc_id is not None:
                utils.mandatory_parameters(dict(
                    colname_site_id=colname_site_id,
                    colname_subc_id=colname_subc_id),
                    additional_message=" (As you provided a CSV file and a subc_id column.)")
            else:
                utils.mandatory_parameters(dict(
                    colname_site_id=colname_site_id,
                    colname_lon=colname_lon,
                    colname_lat=colname_lat),
                    additional_message=" (As you provided a CSV file.)")

        LOGGER.debug(f'User requested ids: {which_ids}')
        possible_ids = ['subc_id', 'basin_id', 'reg_id']
        if not all([some_id in possible_ids for some_id in which_ids]):
            err_msg = (
                f'The requested ids have to be one or several of:'
                f' {possible_ids} (you provided {which_ids})'
            )
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)


        ##########################
        ### Actual computation ###
        ##########################

        ## Potential outputs:
        output_json = None
        output_df = None

        ## Handle GeoJSON case:
        if points_geojson is not None:

            # Note: basin_id and subc_id are queried in the same step,
            # so we don't save anything by treating those separately.
            if 'subc_id' in which_ids or 'basin_id' in which_ids:

                if result_format == 'csv':
                    output_df = basic_queries.get_subcid_basinid_regid_for_geojson(
                        conn, points_geojson, colname_site_id=None)

                elif result_format == 'json':
                    # This returns just plain JSON, not GeoJSON!
                    # TODO Deprecated, contains loop
                    output_json = basic_queries.get_subcid_basinid_regid_for_all_2json(
                        conn, LOGGER, points_geojson, colname_site_id)

                elif result_format == 'geojson':
                    err_msg = "Currently not allowed: geojson output." # TODO
                    LOGGER.debug(err_msg)
                    raise NotImplementedError(err_msg)

            elif 'reg_id' in which_ids:

                if result_format == 'csv':
                    output_df = basic_queries.get_regid_for_geojson(conn, points_geojson, colname_site_id=None)

                else:
                    err_msg = "Currently not allowed: (geo)json output, when getting reg_id only." # TODO
                    LOGGER.debug(err_msg)
                    raise NotImplementedError(err_msg)


        ## Handle CSV case:
        elif input_df is not None:

            if result_format == 'json' or result_format == 'geojson':
                err_msg = "Currently not allowed: (geo)json output for CSV input." # TODO
                LOGGER.debug(err_msg)
                raise NotImplementedError(err_msg)

            # Special case: User provided CSV containing subc_ids, wants basin_ids and reg_ids
            if colname_subc_id is not None:
                # TODO Deprecated, contains loop
                output_df = basic_queries.get_basinid_regid_for_all_from_subcid_1csv(
                    conn, LOGGER, input_df, colname_subc_id, colname_site_id)

            # Note: basin_id and subc_id are queried in the same step,
            # so we don't gain anything by treating those separately.
            elif 'subc_id' in which_ids or 'basin_id' in which_ids:
                # Returns a dataframe with lon, lat, subc_id, basin_id, reg_id, possibly site_id
                # without loop:
                output_df = basic_queries.get_subcid_basinid_regid_for_dataframe(
                    conn, input_df, colname_lon, colname_lat, colname_site_id=colname_site_id)

            elif 'reg_id' in which_ids:
                # Returns a dataframe with lon, lat, reg_id, possibly site_id
                # without loop:
                output_df = basic_queries.get_regid_for_dataframe(
                    conn, input_df, colname_lon, colname_lat, colname_site_id=colname_site_id)



        #####################
        ### Return result ###
        #####################

        return self.return_results('local_ids', requested_outputs, output_df=output_df, output_json=output_json, comment=comment)


if __name__ == '__main__':

    import os
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-local-ids-plural'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson

    ########################
    ### input+output CSV ###
    ########################

    print('TEST CASE 1: input: CSV, output: CSV, reg_id only...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lat": "latitude",
            "colname_lon": "longitude",
            "colname_site_id": "site_id",
            "which_ids": "reg_id",
            "comment": "test1"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    print('TEST CASE 2: input: CSV, output: CSV, all ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lat": "latitude",
            "colname_lon": "longitude",
            "colname_site_id": "site_id",
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test2"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    print('TEST CASE 3: input CSV (with subc_ids), and output: CSV, all ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus_with_subcid.csv",
            "colname_lat": "latitude",
            "colname_lon": "longitude",
            "colname_site_id": "site_id",
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test3"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    ############################
    ### input+output GeoJSON ###
    ############################

    #print('TEST CASE 4: input: GeoJSON (GeometryCollection), output: GeoJSON directly, reg_id only...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [20.087421, 39.364848]
                    },
                    {
                        "type": "Point",
                        "coordinates": [27.846357, 36.548812]
                    },
                    {
                        "type": "Point",
                        "coordinates": [25.73764, 35.24806]
                    },
                    {
                        "type": "Point",
                        "coordinates": [24.17569, 35.50542]
                    }
                ]
            },
            "which_ids": ["reg_id"],
            "comment": "test4"
        }
    }
    #resp = make_sync_request(PYSERVER, process_id, payload)
    #sanity_checks_geojson(resp)

    #print('TEST CASE 5: input: GeoJSON (GeometryCollection), output: GeoJSON directly, reg_id only...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [20.087421, 39.364848]
                    },
                    {
                        "type": "Point",
                        "coordinates": [27.846357, 36.548812]
                    },
                    {
                        "type": "Point",
                        "coordinates": [25.73764, 35.24806]
                    },
                    {
                        "type": "Point",
                        "coordinates": [24.17569, 35.50542]
                    }
                ]
            },
            "result_format": "csv",
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test5"
        }
    }
    #resp = make_sync_request(PYSERVER, process_id, payload)
    #sanity_checks_geojson(resp)

    #print('TEST CASE 5b: input: GeoJSON (GeometryCollection), output: GeoJSON file, all ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [20.087421, 39.364848]
                    },
                    {
                        "type": "Point",
                        "coordinates": [27.846357, 36.548812]
                    },
                    {
                        "type": "Point",
                        "coordinates": [25.73764, 35.24806]
                    },
                    {
                        "type": "Point",
                        "coordinates": [24.17569, 35.50542]
                    }
                ]
            },
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test5b"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    #resp = make_sync_request(PYSERVER, process_id, payload)
    #sanity_checks_geojson(resp)

    #################################
    ### input GeoJSON, output CSV ###
    #################################

    print('TEST CASE 6: input: GeoJSON (GeometryCollection), output: CSV, reg_id only...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [20.087421, 39.364848]
                    },
                    {
                        "type": "Point",
                        "coordinates": [27.846357, 36.548812]
                    },
                    {
                        "type": "Point",
                        "coordinates": [25.73764, 35.24806]
                    },
                    {
                        "type": "Point",
                        "coordinates": [24.17569, 35.50542]
                    }
                ]
            },
            "which_ids": ["reg_id"],
            "result_format": "csv",
            "comment": "test6"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    print('TEST CASE 7: input: GeoJSON (GeometryCollection), output: CSV, all ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [20.087421, 39.364848]
                    },
                    {
                        "type": "Point",
                        "coordinates": [27.846357, 36.548812]
                    },
                    {
                        "type": "Point",
                        "coordinates": [25.73764, 35.24806]
                    },
                    {
                        "type": "Point",
                        "coordinates": [24.17569, 35.50542]
                    }
                ]
            },
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "result_format": "csv",
            "comment": "test7"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    ##############################
    ### input CSV, output JSON ###
    ##############################

    #print('TEST CASE 8: input: CSV, output: Plain JSON directly, reg_id only...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "which_ids": "reg_id",
            "result_format": "json",
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lat": "latitude",
            "colname_lon": "longitude",
            "colname_site_id": "site_id",
            "comment": "test8"
        }
    }
    #resp = make_sync_request(PYSERVER, process_id, payload)
    #sanity_checks_basic(resp)

    #print('TEST CASE 9: input: CSV, output: Plain JSON directly, all ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "result_format": "json",
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lat": "latitude",
            "colname_lon": "longitude",
            "colname_site_id": "site_id",
            "comment": "test9"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    #resp = make_sync_request(PYSERVER, process_id, payload)
    #sanity_checks_basic(resp)

    #print('TEST CASE 10: input: CSV (with subc_ids), output: Plain JSON directly, all ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "result_format": "json",
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus_with_subcid.csv",
            "colname_lat": "latitude",
            "colname_lon": "longitude",
            "colname_site_id": "site_id",
            "comment": "test10"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    #resp = make_sync_request(PYSERVER, process_id, payload)
    #sanity_checks_basic(resp)

    ##################################
    ### input GeoJSON, output JSON ###
    ##################################

    #print('TEST CASE 11: input: GeoJSON (GeometryCollection), output: Plain JSON, reg_id only...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "result_format": "json",
            "which_ids": "reg_id",
            "comment": "test11",
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [20.087421, 39.364848]
                    },
                    {
                        "type": "Point",
                        "coordinates": [27.846357, 36.548812]
                    },
                    {
                        "type": "Point",
                        "coordinates": [25.73764, 35.24806]
                    },
                    {
                        "type": "Point",
                        "coordinates": [24.17569, 35.50542]
                    }
                ]
            }
        }
    }
    #resp = make_sync_request(PYSERVER, process_id, payload)
    #sanity_checks_basic(resp)

    print('TEST CASE 12: (LOOPING) input: GeoJSON (GeometryCollection), output: Plain JSON, all ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "result_format": "json",
            "which_ids": ["subc_id", "basin_id", "reg_id"],
            "comment": "test12",
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [20.087421, 39.364848]
                    },
                    {
                        "type": "Point",
                        "coordinates": [27.846357, 36.548812]
                    },
                    {
                        "type": "Point",
                        "coordinates": [25.73764, 35.24806]
                    },
                    {
                        "type": "Point",
                        "coordinates": [24.17569, 35.50542]
                    }
                ]
            }
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
