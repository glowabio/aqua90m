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
curl -X POST "http://localhost:5000/processes/get-snapped-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "true",
    "comment": "schlei-bei-rabenholz"
    }
}'

# Request a FeatureCollection (Point, 2x LineString, Polygon):
curl -X POST "http://localhost:5000/processes/get-snapped-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "false",
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


class SnappedPointsGetterPlus(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None
        self.config = None

        # Set config:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            self.config = json.load(config_file)

    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<SnappedPointsGetter> {self.name}'


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
        lon = float(data.get('lon'))
        lat = float(data.get('lat'))
        geometry_only = data.get('geometry_only', 'false')
        comment = data.get('comment') # optional

        # Parse booleans
        geometry_only = (geometry_only.lower() == 'true')

        # Get reg_id, basin_id, subc_id, upstream_ids
        LOGGER.info('START: Getting snapped point for lon, lat: %s, %s (or subc_id NONE)' % (lon, lat))
        subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
            conn, LOGGER, lon, lat, None)

        # Get snapped point:
        LOGGER.debug('... Now, getting snapped point and friends for subc_id: %s' % subc_id)

        # Return only geometry:
        if geometry_only:

            geometry_coll = snapping.get_snapped_point_geometry_coll(
                conn, lon, lat, subc_id, basin_id, reg_id)

            if comment is not None:
                geometry_coll['comment'] = comment

            if utils.return_hyperlink('snapped_point', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('snapped_point', geometry_coll,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', geometry_coll


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

            if comment is not None:
                feature_coll['comment'] = comment

            if utils.return_hyperlink('snapped_point', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('snapped_point', feature_coll,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', feature_coll

