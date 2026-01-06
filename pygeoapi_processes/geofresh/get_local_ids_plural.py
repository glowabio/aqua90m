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
        which_ids = data.get('which_ids', ['subc_id', 'basin_id', 'reg_id'])

        ## Check user inputs:
        if not isinstance(which_ids, list) and isinstance(which_ids, str):
            # If user did not put the word into a list...
            which_ids = [which_ids]

        if csv_url is not None and colname_site_id is None:
            LOGGER.error("Missing parameter: colname_site_id")
            err_msg = "Please provide the column name of the site ids inside your csv file (parameter colname_site_id)."
            raise ProcessorExecuteError(err_msg)
            # Note: colname_site_id is also needed if the user provided GeoJSON of
            # type FeatureCollection (instead of type GeometryCollection).

        LOGGER.debug(f'User requested ids: {which_ids}')
        possible_ids = ['subc_id', 'basin_id', 'reg_id']
        if not all([some_id in possible_ids for some_id in which_ids]):
            err_msg = "The requested ids have to be one or several of: %s (you provided %s)" % (possible_ids, which_ids)
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)


        ## Download GeoJSON if user provided URL:
        if points_geojson_url is not None:
            points_geojson = utils.download_geojson(points_geojson_url)


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
                output_json = basic_queries.get_subcid_basinid_regid_for_all_2json(
                    conn, LOGGER, points_geojson, colname_site_id)
            elif 'basin_id' in which_ids:
                err_msg = "Currently, for GeoJSON input, only all ids can be returned. Please set which_ids to [subc_id,basin_id,reg_id], or input a CSV file."
                raise NotImplementedError(err_msg) # TODO
            elif 'reg_id' in which_ids:
                err_msg = "Currently, for GeoJSON input, only all ids can be returned. Please set which_ids to [subc_id,basin_id,reg_id], or input a CSV file."
                raise NotImplementedError(err_msg) # TODO
            # Note: The case where users input subc_ids and want basin_id and reg_id cannot be
            # handled in GeoJSON, as GeoJSON must by definition contain coordinates!
            # In this case, instead of GeoJSON, we'd have to let them input just some dictionary.
            # Probably not needed ever, so why implement that. It would just be for completeness.
            # Implement when needed.

        ## Handle CSV case:
        elif csv_url is not None:
            input_df = utils.access_csv_as_dataframe(csv_url)

            # Query database:
            if colname_subc_id is not None:
                # Special case! User provided CSV with a column containing subc_ids...
                output_df = basic_queries.get_basinid_regid_for_all_from_subcid_1csv(
                    conn, LOGGER, input_df, colname_subc_id, colname_site_id)

            elif 'subc_id' in which_ids:
                output_df = basic_queries.get_subcid_basinid_regid_for_all_1csv( # TODO: make int!
                    conn, LOGGER, input_df, colname_lon, colname_lat, colname_site_id)
            elif 'basin_id' in which_ids:
                output_df = basic_queries.get_basinid_regid_for_all_1csv(# TODO: make int!
                    conn, LOGGER, input_df, colname_lon, colname_lat, colname_site_id)
            elif 'reg_id' in which_ids:
                output_df = basic_queries.get_regid_for_all_1csv(# TODO: make int!
                    conn, LOGGER, input_df, colname_lon, colname_lat, colname_site_id)

        else:
            err_msg = 'Please provide either GeoJSON (points_geojson, points_geojson_url) or CSV data (csv_url).'
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)


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


    ## Test 1
    print('TEST CASE 1: CSV input and output...', end="", flush=True)  # no newline
    payload = {
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
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)

    ## Test 2
    print('TEST CASE 2: GeoJSON input (GeometryCollection) and output...', end="", flush=True)  # no newline
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
            "comment": "schlei-near-rabenholz"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


