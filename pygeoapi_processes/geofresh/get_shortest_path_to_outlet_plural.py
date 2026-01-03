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
        "return_csv": true
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
        "return_csv": true
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
        "return_json": true
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

        # Option 1:
        # Input:  CSV with lon, lat (or with subc_id)
        # Output: CSV with added columns containing a list of the downstream ids... (TODO: Not ideal as format!)

        # Option 2 (to be implemented, TODO)
        # Input:  GeoJSON with points
        # Output: GeoJSON with points, and for each point, a list of the downstream ids

        # User inputs
        return_csv  = data.get('return_csv', None)
        return_json = data.get('return_json', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', 'lon')
        colname_lat = data.get('colname_lat', 'lat')
        colname_site_id = data.get('colname_site_id', None)
        comment = data.get('comment', None)
        geometry_only = data.get('geometry_only', False)
        downstream_ids_only = data.get('downstream_ids_only', False)
        add_downstream_ids = data.get('add_downstream_ids', False)
        # GeoJSON:
        points_geojson = None # TODO

        ########################
        ### Check parameters ###
        ########################

        utils.exactly_one_param(dict(return_csv=return_csv, return_json=return_json))

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

        if csv_url is not None and colname_site_id is None:
            err_msg = "If you provide a CSV file, you must provide colname_site_id!"
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
        elif csv_url is not None:

            # Download CSV:
            LOGGER.debug(f'Accessing input CSV from: {csv_url}')
            input_df = utils.access_csv_as_dataframe(csv_url)
            LOGGER.debug('Accessing input CSV... DONE. Found {ncols} columns (names: {colnames})'.format(
                ncols=input_df.shape[1], colnames=input_df.columns))

            ## Now, for each row, get the ids (unless already present)!
            if not (colname_site_id in input_df.columns):
                err_msg = "Please add a column 'site_id' to your input dataframe."
                LOGGER.error(err_msg)
                raise ProcessorExecuteError(err_msg)
            elif (('subc_id' in input_df.columns) and
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
                temp_df = basic_queries.get_basinid_regid_from_subcid_plural(conn, LOGGER, subc_ids)
                # Join back to input dataframe to add the site_ids:
                temp_df = pd.merge(input_df, temp_df, on="subc_id")
            else:
                LOGGER.debug('Querying required columns (subc_id, basin_id, reg_id) for each point...')
                temp_df = basic_queries.get_subcid_basinid_regid_for_dataframe(
                    conn, 'shortestpath', input_df, colname_lon, colname_lat, colname_site_id)

            ## Next, for each row, get the downstream ids!
            if return_csv:
                output_df = routing.get_dijkstra_ids_to_outlet_plural(conn, temp_df, colname_site_id, return_csv=True)
            elif return_json:
                output_json = routing.get_dijkstra_ids_to_outlet_plural(conn, temp_df, colname_site_id, return_json=True)


        #####################
        ### Return result ###
        #####################

        return self.return_results('downstream_path', requested_outputs, output_df=output_df, output_json=output_json, comment=comment)
