import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
import pygeoapi.process.aqua90m.utils.exceptions as exc
import pygeoapi.process.aqua90m.geofresh.snapping_strahler as snapping_strahler
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config



'''
INPUT: CSV
OUTPUT: CSV
curl -X POST "https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
    "colname_lon": "longitude",
    "colname_lat": "latitude",
    "colname_site_id": "site_id",
    "min_strahler": 5,
    "add_distance": true
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

INPUT: CSV
OUTPUT: JSON
curl -X POST "https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
    "colname_lon": "longitude",
    "colname_lat": "latitude",
    "colname_site_id": "site_id",
    "result_format": "geojson",
    "min_strahler": 5,
    "add_distance": true
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'


INPUT:  JSON
OUTPUT: JSON
curl -X POST "https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
    "colname_site_id": "my_site",
    "result_format": "geojson",
    "min_strahler": 5,
    "add_distance": true
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'


# INPUT: MultiPoint
# OUTPUT: FeatureCollection
curl -X POST "https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "min_strahler": 5,
    "add_distance": true,
    "points_geojson": {
      "type": "MultiPoint",
      "coordinates": [
        [9.937520027160646, 54.69422745526058],
        [9.9217, 54.6917],
        [9.9312, 54.6933]
      ]
    }
  }
}'

# INPUT: FeatureCollection
# OUTPUT: FeatureCollection
curl -X POST "https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "colname_site_id": "my_site",
    "min_strahler": 5,
    "add_distance": true,
    "points_geojson": {
        "type": "FeatureCollection",
        "features": [
            {
               "type": "Feature",
               "geometry": { "type": "Point", "coordinates": [9.931555, 54.695070]},
               "properties": {
                   "my_site": "bla1",
                   "species_name": "Hase",
                   "species_id": "007"
               }
            },
            {
               "type": "Feature",
               "geometry": { "type": "Point", "coordinates": [9.921555, 54.295070]},
               "properties": {
                   "my_site": "bla2",
                   "species_name": "Delphin",
                   "species_id": "008"
               }
            }
        ]
    }
  }
}'

# INPUT: FeatureCollection
# OUTPUT: CSV
curl -X POST "https://$PYSERVER/processes/get-snapped-points-strahler-plural/execution" \
--header "Content-Type: application/json" \
--data '{
  "outputs": {
    "transmissionMode": "reference"
  },
  "inputs": {
    "colname_site_id": "my_site",
    "min_strahler": 5,
    "add_distance": true,
    "result_format": "csv",
    "colname_lon": "long_wgs84",
    "colname_lat": "lat_wgs84",
    "points_geojson": {
        "type": "FeatureCollection",
        "features": [
            {
               "type": "Feature",
               "geometry": { "type": "Point", "coordinates": [9.931555, 54.695070]},
               "properties": {
                   "my_site": "bla1",
                   "species_name": "Hase",
                   "species_id": "007"
               }
            },
            {
               "type": "Feature",
               "geometry": { "type": "Point", "coordinates": [9.921555, 54.295070]},
               "properties": {
                   "my_site": "bla2",
                   "species_name": "Delphin",
                   "species_id": "008"
               }
            }
        ]
    }
  }
}'
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class SnappedPointsStrahlerGetterPlural(BaseProcessor):

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
        return f'<SnappedPointsStrahlerGetterPlural> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.debug('Start execution: %s (job %s)' % (self.metadata['id'], self.job_id))
        LOGGER.debug('Inputs: %s' % data)
        LOGGER.log(logging.TRACE, 'Requested outputs: %s' % outputs)

        try:
            conn = get_connection_object_config(self.config)
            res = self._execute(data, outputs, conn)
            LOGGER.debug('Finished execution: %s (job %s)' % (self.metadata['id'], self.job_id))
            LOGGER.log(logging.TRACE, 'Closing connection...')
            conn.close()
            LOGGER.log(logging.TRACE, 'Closing connection... Done.')
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

        # User inputs
        #input_points_geojson = data.get('points')
        #geometry_only = data.get('geometry_only', 'false')
        #comment = data.get('comment') # optional

        # User inputs:
        min_strahler = data.get('min_strahler', None)
        add_distance = data.get('add_distance', None)
        # GeoJSON, posted directly
        points_geojson = data.get('points_geojson', None)
        # GeoJSON, to be downloaded via URL:
        points_geojson_url = data.get('points_geojson_url', None)
        # CSV, to be downloaded via URL
        csv_url = data.get('csv_url', None)
        colname_lon = data.get('colname_lon', 'lon')
        colname_lat = data.get('colname_lat', 'lat')
        colname_site_id = data.get('colname_site_id', 'site_id')
        # Ask for result format
        result_format = data.get('result_format', None)
        # Optional comment:
        comment = data.get('comment') # optional

        if min_strahler is None:
            err_msg = "Missing parameter 'min_strahler'. Please provide an integer value."
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

        if add_distance is None:
            err_msg = "Missing parameter 'add_distance'. Please provide a boolean value."
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)


        ## Potential outputs:
        output_json = None
        output_df = None


        ## Check which format
        if result_format is None:
            if points_geojson is not None or points_geojson_url is not None:
                LOGGER.debug('User did not specify output format, but provided GeoJSON, so we will provide geojson back!')
                result_format = 'geojson'
            elif csv_url is not None:
                LOGGER.debug('User did not specify output format, but provided CSV, so we will provide CSV back!')
                result_format = 'csv'

        ## Validate output format:
        if result_format not in ['csv', 'geojson']:
            err_msg = f'Wrong result format: {result_format}!'
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)


        ## Download GeoJSON if user provided URL:
        if points_geojson_url is not None:
            points_geojson = utils.download_geojson(points_geojson_url)

        ## Handle GeoJSON case:
        if points_geojson is not None:

            # If a FeatureCollections is passed, check whether the property "site_id" (or similar)
            # is present in every feature:
            if points_geojson['type'] == 'FeatureCollection':
                geojson_helpers.check_feature_collection_property(points_geojson, colname_site_id)

            # Query database:
            if result_format == 'geojson':
                LOGGER.debug('Requesting geojson (get_snapped_points_json2json)')
                output_json = snapping_strahler.get_snapped_points_json2json(conn, points_geojson, min_strahler, colname_site_id = colname_site_id, add_distance=add_distance)
            elif result_format == 'csv':
                LOGGER.debug('Requesting csv (get_snapped_points_json2csv)')
                output_df = snapping_strahler.get_snapped_points_json2csv(conn, points_geojson, min_strahler, colname_lon, colname_lat, colname_site_id, add_distance=add_distance)

        ## Handle CSV case:
        elif csv_url is not None:
            input_df = utils.access_csv_as_dataframe(csv_url)

            # Query database:
            LOGGER.info(f'PYGEOAPI USER PASSED STRAHLER {min_strahler}')
            if result_format == 'geojson':
                LOGGER.debug('Requesting geojson (get_snapped_points_csv2json)')
                output_json = snapping_strahler.get_snapped_points_csv2json(conn, input_df, min_strahler, colname_lon, colname_lat, colname_site_id, add_distance=add_distance)
            elif result_format == 'csv':
                LOGGER.debug('Requesting csv (get_snapped_points_csv2csv)')
                output_df = snapping_strahler.get_snapped_points_csv2csv(conn, input_df, min_strahler, colname_lon, colname_lat, colname_site_id, add_distance=add_distance)

        else:
            err_msg = 'Please provide either GeoJSON (points_geojson, points_geojson_url) or CSV data (csv_url).'
            LOGGER.error(err_msg)
            raise exc.UserInputException(err_msg)



        #####################
        ### Return result ###
        #####################

        do_return_link = utils.return_hyperlink('snapped_points', requested_outputs)

        ## Return CSV:
        if output_df is not None:
            if do_return_link:
                output_dict_with_url =  utils.store_to_csv_file('snapped_points', output_df,
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
                output_dict_with_url =  utils.store_to_json_file('snapped_points', output_json,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url

            else:
                return 'application/json', output_json


