import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
import pygeoapi.process.aqua90m.utils.exceptions as exc
import pygeoapi.process.aqua90m.geofresh.snapping_strahler as snapping_strahler
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config



'''
# INPUT:  CSV
# OUTPUT: CSV
# Tested 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
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

# INPUT:  CSV File
# OUTPUT: GeoJSON File (FeatureCollection)
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
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

# INPUT:  GeoJSON File (FeatureCollection)
# OUTPUT: GeoJSON File (FeatureCollection)
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
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

# INPUT:  GeoJSON File (GeometryCollection)
# OUTPUT: GeoJSON File (FeatureCollection)
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometrycollection_points.json",
    "colname_site_id": "my_site",
    "result_format": "geojson",
    "min_strahler": 5,
    "add_distance": true
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

# Requesting no distance
# INPUT:  GeoJSON File (GeometryCollection)
# OUTPUT: GeoJSON File (FeatureCollection)
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometrycollection_points.json",
    "result_format": "geojson",
    "min_strahler": 5,
    "add_distance": false
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

# Requesting no distance
# INPUT:  GeoJSON directly (GeometryCollection)
# OUTPUT: GeoJSON directly (FeatureCollection)
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_geojson": {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "Point",
                "coordinates": [9.931555, 54.695070]
            },
            {
                "type": "Point",
                "coordinates": [9.921555, 54.295070]
            }
        ]
    },
    "result_format": "geojson",
    "min_strahler": 5,
    "add_distance": false
  }
}'

# INPUT:  GeoJSON directly (MultiPoint)
# OUTPUT: GeoJSON directly (FeatureCollection)
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
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

# INPUT:  GeoJSON directly (FeatureCollection)
# OUTPUT: GeoJSON directly (FeatureCollection)
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
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

# INPUT:  GeoJSON directly (FeatureCollection)
# OUTPUT: CSV File
# Tested 2026-01-02 (commit 38cf498ca)
curl -X POST https://${PYSERVER}/processes/get-snapped-points-strahler-plural/execution \
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


class SnappedPointsStrahlerGetterPlural(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        #input_points_geojson = data.get('points')
        #geometry_only = data.get('geometry_only', False)
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

        ## Check if boolean:
        utils.is_bool_parameters(dict(add_distance=add_distance))


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

        return self.return_results('snapped_points', requested_outputs, output_df=output_df, output_json=output_json, comment=comment)



if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-snapped-points-strahler-plural'
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
            "min_strahler": 5,
            "add_distance": True,
            "comment": "test1"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 2: Cross: Input CSV file, output GeoJSON file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "csv_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/spdata_barbus.csv",
            "colname_lon": "longitude",
            "colname_lat": "latitude",
            "colname_site_id": "site_id",
            "result_format": "geojson",
            "min_strahler": 5,
            "add_distance": True,
            "comment": "test2"
        },
          "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 3: Input GeoJSON file (FeatureCollection), output GeoJSON file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
            "colname_site_id": "my_site",
            "result_format": "geojson",
            "min_strahler": 5,
            "add_distance": True,
            "comment": "test3"
        },
          "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 4: Input GeoJSON file (GeometryCollection), output GeoJSON file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometrycollection_points.json",
            "colname_site_id": "my_site",
            "result_format": "geojson",
            "min_strahler": 5,
            "add_distance": True,
            "comment": "test4"
        },
          "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 5: Like test case 4 but no distance', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_geometrycollection_points.json",
            "colname_site_id": "my_site",
            "result_format": "geojson",
            "min_strahler": 5,
            "add_distance": False,
            "comment": "test5"
        },
          "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 6: Input GeoJSON directly (GeometryCollection), output GeoJSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [9.931555, 54.695070]
                    },
                    {
                        "type": "Point",
                        "coordinates": [9.921555, 54.295070]
                    }
                ]
            },
            "result_format": "geojson",
            "min_strahler": 5,
            "add_distance": False,
            "comment": "test6"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 7: Input GeoJSON directly (MultiPoint), output GeoJSON directly...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "min_strahler": 5,
            "add_distance": True,
            "points_geojson": {
                "type": "MultiPoint",
                "coordinates": [
                    [9.937520027160646, 54.69422745526058],
                    [9.9217, 54.6917],
                    [9.9312, 54.6933]
                ]
            },
            "comment": "test7"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 8: Input GeoJSON directly (FeatureCollection), output GeoJSON directly (FeatureCollection)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "colname_site_id": "my_site",
            "min_strahler": 5,
            "add_distance": True,
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
            },
            "comment": "test8"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 9: Cross: Input GeoJSON directly (FeatureCollection), output CSV file...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "result_format": "csv",
            "colname_lon": "long_wgs84",
            "colname_lat": "lat_wgs84",
            "colname_site_id": "my_site",
            "min_strahler": 5,
            "add_distance": True,
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
            },
            "comment": "test9"
        },
        "outputs": {
            "transmissionMode": "reference"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
