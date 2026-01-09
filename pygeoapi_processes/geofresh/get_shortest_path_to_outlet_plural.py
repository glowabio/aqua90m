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
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.utils.exceptions as exc
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
## INPUT:  CSV file
## OUTPUT: CSV file
## Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-to-outlet-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "colname_lon": "longitude",
        "colname_lat": "latitude",
        "colname_site_id": "site_id",
        "downstream_ids_only": true,
        "result_format": "csv"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

## INPUT:  CSV file
## OUTPUT: CSV file
## Tested: 2026-01-02
## This contains subc_ids, so they will be used instead of lat lon... TODO Is this desired?
curl -X POST https://${PYSERVER}/processes/get-shortest-path-to-outlet-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus_with_subcid.csv",
        "colname_lon": "longitude",
        "colname_lat": "latitude",
        "colname_site_id": "site_id",
        "downstream_ids_only": true,
        "result_format": "csv"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

## INPUT:  CSV file
## OUTPUT: Plain JSON file
## Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-to-outlet-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
        "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
        "colname_lon": "longitude",
        "colname_lat": "latitude",
        "colname_site_id": "site_id",
        "downstream_ids_only": true,
        "result_format": "json"
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


class ShortestPathToOutletGetterPlural(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        # Input options:
        # Input: CSV with lon, lat (or with subc_id)
        # Input: GeoJSON FeatureCollection with points

        # Output options:
        # Output: CSV with added columns containing a list of the downstream ids... (TODO: Not ideal as format!)
        # Output: UGLY JSON???
        # Output: TODO: GeoJSON with points, and for each point, a list of the downstream ids

        # Plural case:
        # GeoJSON (e.g. Multipoint, GeometryCollection of Points,
        # FeatureCollection of Points), posted directly:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', 'lon')
        colname_lat = data.get('colname_lat', 'lat')
        colname_site_id = data.get('colname_site_id', None)
        geometry_only = data.get('geometry_only', False)
        downstream_ids_only = data.get('downstream_ids_only', False)
        add_downstream_ids = data.get('add_downstream_ids', False)
        result_format = data.get('result_format', None)
        comment = data.get('comment', None)

        ##############################
        ### Download if applicable ###
        ### and validate GeoJSON   ###
        ##############################

        utils.mandatory_parameters(dict(colname_site_id=colname_site_id))

        if points_geojson_url is not None:
            points_geojson = utils.download_geojson(points_geojson_url)

        if points_geojson is not None:

            # Check if FeatureCollection:
            if not points_geojson['type'] == 'FeatureCollection':
                err_msg = f"Input GeoJSON has to be a FeatureCollection, not '{points_geojson['type']}'."
                raise ProcessorExecuteError(err_msg)

            # Check if every feature has id:
            geojson_helpers.check_feature_collection_property(points_geojson, colname_site_id)

        input_df = None
        if csv_url is not None:
            input_df = utils.access_csv_as_dataframe(csv_url)
            LOGGER.debug('Input CSV: Found {ncols} columns (names: {colnames})'.format(
                ncols=input_df.shape[1], colnames=input_df.columns))

            # Check if every row has id:
            if not (colname_site_id in input_df.columns):
                err_msg = "Please add a column 'site_id' to your input dataframe."
                LOGGER.error(err_msg)
                raise ProcessorExecuteError(err_msg)


        #################################
        ### Validate input parameters ###
        #################################

        # Check result format
        if not (result_format is None or result_format == 'json' or result_format == 'csv'):
            err_msg = "Malformed parameter: result_format can only be 'csv' or 'json', not {result_format}"
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

        # Check if boolean:
        utils.is_bool_parameters(dict(
            geometry_only=geometry_only,
            downstream_ids_only=downstream_ids_only,
            add_downstream_ids=add_downstream_ids
        ))

        if not downstream_ids_only:
            err_msg = "Cannot return geometries for CSV input yet! (And probably never will, because returning geometry inside a CSV does not make sense...)"
            LOGGER.error(err_msg)
            raise NotImplementedError(err_msg)
            # TODO: Any idea how to return linestrings in a csv? Is that required, or even desired at all?

        if geometry_only:
            err_msg = "geometry_only: Returning geometry is not supported yet."
            LOGGER.error(err_msg)
            raise NotImplementedError(err_msg)

        if add_downstream_ids:
            err_msg = "geometry_only: Returning geometry with added downstream ids is not supported yet."
            LOGGER.error(err_msg)
            raise NotImplementedError(err_msg)

        # If user specified no output format, will use the input format...
        if result_format is None and csv_url is not None:
            result_format = "csv"
        elif result_format is None and points is not None:
            result_format = "json"


        ##########################
        ### Actual computation ###
        ##########################
        # Overall goal: Get the dijkstra shortest path (as linestrings)!

        ## Potential outputs:
        output_df_or_json = None

        ## Handle GeoJSON case:
        if points_geojson is not None:

            # Check if the required properties "subc_id",
            # "basin_id", "reg_id" are already present.
            try:
                geojson_helpers.check_feature_collection_property(points_geojson, "subc_id")
                geojson_helpers.check_feature_collection_property(points_geojson, "basin_id")
                geojson_helpers.check_feature_collection_property(points_geojson, "reg_id")
                LOGGER.info(
                    'Input FeatureCollection already contains required properties'
                    ' (subc_id, basin_id, reg_id) for each Feature, using that...'
                )
                # Actual routing: For each feature, get the downstream ids!
                # In this case, we call the routing method with GeoJSON input:
                output_df_or_json = routing.get_dijkstra_ids_to_outlet_plural(
                    conn,
                    points_geojson,
                    colname_site_id,
                    result_format
                )

            # This is the normal case: "subc_id", "basin_id" and
            # "reg_id" have to be retrieved.
            except exc.UserInputException as e:
                # For each feature, retrieve the required ids "subc_id", "basin_id", "reg_id":
                # (Note: Instead, we could use "add_subcid_basinid_regid_to_featurecoll" which
                # outputs a FeatureCollection, that is easier to understand, but slower.)
                temp_df = basic_queries.get_subcid_basinid_regid_for_geojson(
                    conn,
                    points_geojson,
                    colname_site_id=colname_site_id
                )
                # Now that a dataframe was created from Database output,
                # the column name for the site_ids has changed:
                colname_site_id = 'site_id'

                # Actual routing: For each item, get the downstream ids!
                output_df_or_json = routing.get_dijkstra_ids_to_outlet_plural(
                    conn,
                    temp_df,
                    colname_site_id,
                    result_format
                )

        ## Handle CSV case:
        elif input_df is not None:

            ## For each row, get the ids (unless already present)!
            if (('subc_id' in input_df.columns) and
                  ('basin_id' in input_df.columns) and
                  ('reg_id' in input_df.columns) and
                  (colname_site_id  in input_df.columns)):
                LOGGER.debug('Input dataframe already contains required columns (subc_id, basin_id, reg_id) for each point, using that...')
                temp_df = input_df
            elif ('subc_id' in input_df.columns):
                LOGGER.debug('Input dataframe already contains column subc_id, querying basin_id and reg_id for them...')
                # This case is maybe not needed. Instead, users should send their stuff through get_ids in the beginning,
                # during/after snapping.
                subc_ids = input_df['subc_id'].astype(int).tolist()
                temp_df = basic_queries.get_basinid_regid_from_subcid_plural(conn, subc_ids)
                # Join back to input dataframe to add the site_ids:
                temp_df = pd.merge(input_df, temp_df, on="subc_id")
            else:
                LOGGER.debug('Querying required columns (subc_id, basin_id, reg_id) for each point...')
                temp_df = basic_queries.get_subcid_basinid_regid_for_dataframe(
                    conn, input_df, colname_lon, colname_lat, colname_site_id)

            # Actual routing: For each row, get the downstream ids!
            output_df_or_json = routing.get_dijkstra_ids_to_outlet_plural(
                conn,
                temp_df,
                colname_site_id,
                result_format
            )

        #####################
        ### Return result ###
        #####################

        output_df = output_json = None
        if isinstance(output_df_or_json, pd.DataFrame):
            output_df = output_df_or_json
        elif isinstance(output_df_or_json, dict):
            output_json = output_df_or_json

        return self.return_results('downstream_path', requested_outputs, output_df=output_df, output_json=output_json, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-shortest-path-to-outlet-plural'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Input CSV file, output CSV file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lon": "longitude",
            "colname_lat": "latitude",
            "colname_site_id": "site_id",
            "downstream_ids_only": True,
            "result_format": "csv",
            "comment": "test1"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    ## This contains subc_ids, so they will be used instead of lat lon... TODO Is this desired?
    print('TEST CASE 2: Like test case 1 but based on subc_id...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus_with_subcid.csv",
            "colname_lon": "longitude",
            "colname_lat": "latitude",
            "colname_site_id": "site_id",
            "downstream_ids_only": True,
            "result_format": "csv",
            "comment": "test2"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 3: Input CSV file, output JSON file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lon": "longitude",
            "colname_lat": "latitude",
            "colname_site_id": "site_id",
            "downstream_ids_only": True,
            "result_format": "json",
            "comment": "test3"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 4: Input GeoJSON file, output CSV file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
            "colname_site_id": "my_site",
            "downstream_ids_only": True,
            "result_format": "csv",
            "comment": "test4"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 5: Input GeoJSON file, output JSON file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
            "colname_site_id": "my_site",
            "downstream_ids_only": True,
            "result_format": "json",
            "comment": "test5"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
