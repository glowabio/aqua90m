import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
#import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.outlets as outlets
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
## Without geometry
## INPUT:  GeoJSON directly (Geometry: Polygon)
## OUTPUT: Plain JSON directly
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-outlets-for-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 3,
    "add_geometry": false,
    "comment": "near helsinki",
    "polygon": {
      "type": "Polygon",
      "coordinates": [
        [
          [ 24.99422594742927, 60.122188238921],
          [ 24.99422594742927, 60.287391694733],
          [ 24.52403906370872, 60.287391694733],
          [ 24.52403906370872, 60.122188238921],
          [ 24.99422594742927, 60.122188238921]
        ]
      ]
    }
  }
}'

## With geometry
## INPUT:  GeoJSON directly (Geometry: Polygon)
## OUTPUT: GeoJSON directly (FeatureCollection)
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-outlets-for-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 3,
    "add_geometry": true,
    "comment": "near helsinki",
    "polygon": {
      "type": "Polygon",
      "coordinates": [
        [
          [ 24.99422594742927, 60.122188238921],
          [ 24.99422594742927, 60.287391694733],
          [ 24.52403906370872, 60.287391694733],
          [ 24.52403906370872, 60.122188238921],
          [ 24.99422594742927, 60.122188238921]
        ]
      ]
    }
  }
}'


## With geometry
## INPUT:  GeoJSON File (Geometry: Polygon)
## OUTPUT: GeoJSON File (FeatureCollection)
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-outlets-for-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 3,
    "add_geometry": true,
    "comment": "near helsinki",
    "polygon_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometry_polygon.json"
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

## With geometry
## INPUT:  GeoJSON File (Feature: Polygon)
## OUTPUT: GeoJSON File (FeatureCollection)
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-outlets-for-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 3,
    "add_geometry": true,
    "comment": "near helsinki",
    "polygon_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_feature_polygon.json"
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

## Without geometry
## INPUT:  GeoJSON File (Geometry: Polygon)
## OUTPUT: Plain JSON File
## Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-outlets-for-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 3,
    "add_geometry": false,
    "comment": "near helsinki",
    "polygon_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometry_polygon.json"
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



class OutletGetter(GeoFreshBaseProcessor):


    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        # GeoJSON, posted directly
        polygon_geojson = data.get('polygon', None)
        # GeoJSON, to be downloaded via URL:
        polygon_geojson_url = data.get('polygon_geojson_url', None)
        min_strahler = data.get('min_strahler')
        add_geometry = data.get('add_geometry', False)
        comment = data.get('comment') # optional

        ## Download GeoJSON if user provided URL:
        if polygon_geojson_url is not None:
            polygon_geojson = utils.download_geojson(polygon_geojson_url)
            LOGGER.debug(f'Downloaded GeoJSON: {polygon_geojson}')
            ## Can not use Features:
            if polygon_geojson["type"] == "Feature":
                polygon_geojson = polygon_geojson["geometry"]
                LOGGER.debug(f'Modified GeoJSON: {polygon_geojson}')

        if add_geometry:
            featurecoll = outlets.get_outlet_streamsegments_in_polygon(conn,
                polygon_geojson,
                min_strahler=min_strahler
            )
            LOGGER.debug(f'Found Feature coll...')
            output_json = featurecoll
        else:
            subcids = outlets.get_outlet_subcids_in_polygon(conn,
                polygon_geojson,
                min_strahler=min_strahler
            )
            LOGGER.debug(f'Found subcids: {subcids}')
            subcids = {
                "subc_ids" : subcids
            }
            output_json = subcids


        ################
        ### Results: ###
        ################

        return self.return_results('outlets', requested_outputs, output_df=None, output_json=output_json, comment=comment)


