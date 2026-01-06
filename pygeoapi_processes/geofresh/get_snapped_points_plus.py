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
import pygeoapi.process.aqua90m.geofresh.get_polygons as get_polygons
import pygeoapi.process.aqua90m.geofresh.snapping as snapping
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config

'''
Note:
TODO FIXME:
This should be replaced by using the normal get_snapped_point.py with parameter add_subcatchment,
but then I need to change my test HTML client, which currently only can make different process calls
by using different process id, and not by adding parameters.


# Request a GeometryCollection (Point, 2x LineString):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-snapped-point-plus/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": true,
    "comment": "schlei-bei-rabenholz"
    }
}'

# Request a FeatureCollection (Point, 2x LineString, Polygon):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-snapped-point-plus/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": false,
    "comment": "schlei-bei-rabenholz"
    }
}'

# TODO: FUTURE: If we ever snap to stream segments outside of the immediate subcatchment,
# need to adapt some stuff in this process...

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class SnappedPointsGetterPlus(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        lon = float(data.get('lon'))
        lat = float(data.get('lat'))
        geometry_only = data.get('geometry_only', False)
        comment = data.get('comment') # optional

        # Check if boolean:
        utils.is_bool_parameters(dict(geometry_only=geometry_only))

        # Get reg_id, basin_id, subc_id, upstream_ids
        LOGGER.info(f'START: Getting snapped point for lon, lat: {lon}, {lat}')
        subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
            conn, LOGGER, lon, lat)

        # Get snapped point:
        LOGGER.debug(f'... Now, getting snapped point and friends for subc_id: {subc_id}')

        # Return only geometry:
        if geometry_only:

            geometry_coll = snapping.get_snapped_point_geometry_coll(
                conn, lon, lat, subc_id, basin_id, reg_id)

            return self.return_results('snapped_point', requested_outputs, output_df=None, output_json=geometry_coll, comment=comment)


        # Return feature collection:
        if not geometry_only:

            feature_coll = snapping.get_snapped_point_feature_coll(
                conn, lon, lat, subc_id, basin_id, reg_id)

            # Get strahler order (all features of the above collection contain strahler order):
            strahler = feature_coll["features"][0]["properties"]["strahler"]

            # Get local subcatchment polygon too
            temp_feature_coll = get_polygons.get_subcatchment_polygons_feature_coll(
                conn, [subc_id], basin_id, reg_id)
            subcatchment_feature = temp_feature_coll["features"][0]
            subcatchment_feature['properties']['subc_id'] = subc_id
            subcatchment_feature['properties']['strahler'] = strahler
            subcatchment_feature['properties']['basin_id'] = basin_id
            subcatchment_feature['properties']['reg_id'] = reg_id
            subcatchment_feature['properties']['lon_original'] = lon
            subcatchment_feature['properties']['lat_original'] = lat
            subcatchment_feature['properties']['description'] = "sub_catchment containing the snapped point."
            feature_coll["features"].append(subcatchment_feature)

            return self.return_results('snapped_point', requested_outputs, output_df=None, output_json=feature_coll, comment=comment)
