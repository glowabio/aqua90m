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
        basin_id = data.get('basin_id', None)
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', False)

        # Check type:
        utils.mandatory_parameters(dict(basin_id=basin_id))
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Get basin geometry:
        reg_id = basic_queries.get_regid_from_basinid(conn, LOGGER, basin_id)
        LOGGER.debug(f'Now, getting polygon for basin_id: {basin_id}')
        geojson_item = None

        if geometry_only:
            geojson_item = get_polygons.get_basin_polygon(conn, basin_id, reg_id, make_feature=False)
        else:
            geojson_item = get_polygons.get_basin_polygon(conn, basin_id, reg_id, make_feature=True)

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        return self.return_results('polygon', requested_outputs, output_df=None, output_json=geojson_item, comment=comment)


