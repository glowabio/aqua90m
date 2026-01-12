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
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.get_polygons as get_polygons
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

# Request a FeatureCollection, based on a basin_id:
# Output: Polygon (FeatureCollection)
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-basin-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": false,
    "comment": "close to bremerhaven"
    }
}'

# Request a simple GeometryCollection, based on a basin_id
# Output: Polygon (GeometryCollection)
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-basin-polygon/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": true,
    "comment": "close to bremerhaven"
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class BasinPolygonGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        # User inputs
        # Input can be a basin:
        basin_id = data.get('basin_id', None) # optional, need either lonlat OR subc_id
        # Or a point, from which we infer the basin:
        point = data.get('point', None)
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id  = data.get('subc_id',  None) # optional, need either lonlat OR subc_id
        geometry_only = data.get('geometry_only', False)
        comment = data.get('comment') # optional

        # Check type:
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Check presence:
        utils.at_least_one_param({
            "basin_id": basin_id,
            "subc_id": subc_id,
            "point": point,
            "pair of coordinates (lon and lat)": (lon and lat)
        })

        # If GeoJSON point is given, get coordinates:
        if point is not None:
            lon, lat = point.get('coordinates') or point['geometry']['coordinates']

        # Get id(s) required for querying for geometry:
        if basin_id is not None:
            reg_id = basic_queries.get_regid_from_basinid(conn, LOGGER, basin_id)
        else:
            basin_id, reg_id = basic_queries.get_basinid_regid(conn, LOGGER, lon=lon, lat=lat, subc_id=subc_id):

        LOGGER.debug(f'Now, getting polygon for basin_id: {basin_id}')
        geojson_item = None

        if geometry_only:
            geojson_item = get_polygons.get_basin_polygon(conn, basin_id, reg_id, make_feature=False)
        else:
            geojson_item = get_polygons.get_basin_polygon(conn, basin_id, reg_id, make_feature=True)

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        return self.return_results('polygon', requested_outputs, output_df=None, output_json=geojson_item, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-basin-polygon'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Input: basin_id, output: FeatureCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_id": 1288419,
            "geometry_only": False,
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 2: Input: basin_id, output: GeometryCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_id": 1288419,
            "geometry_only": True,
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)


    print('TEST CASE 3: Input: point, output: GeometryCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "point": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [8.278198242187502, 53.54910661890981]
                }
            },
            "geometry_only": True,
            "comment": "test3"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)

    print('TEST CASE 4: Input: subc_id, output: GeometryCollection...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_id": 506586041,
            "geometry_only": True,
            "comment": "test4"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_geojson(resp)
