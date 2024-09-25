
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import pygeoapi.process.utils.upstream_helpers as helpers
from pygeoapi.process.geofresh.py_query_db import get_connection_object
from pygeoapi.process.geofresh.py_query_db import get_snapped_point_simple
from pygeoapi.process.geofresh.py_query_db import get_polygon_for_subcid_simple
import psycopg2

'''
Note:
TODO FIXME:
This should be replaced by using the normal get_snapped_point.py with parameter add_subcatchment,
but then I need to change my test HTML client, which currently only can make different process calls
by using different process id, and not by adding parameters.

curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-snapped-point/execution" -H "Content-Type: application/json" -d "{\"inputs\":{ \"lon\": 9.931555, \"lat\": 54.695070, \"comment\":\"Nordoestliche Schlei, bei Rabenholz\"}}"

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
        LOGGER.info('Starting to get the snapped point coordinates..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        try:
            conn = self.get_db_connection()
            res = self._execute(data, outputs, conn)

            LOGGER.debug('Closing connection...')
            conn.close()
            LOGGER.debug('Closing connection... Done.')

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
        subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, None)

        # Get snapped point:
        LOGGER.debug('... Now, getting snapped point for subc_id (as simple geometries): %s' % subc_id)
        strahler, snappedpoint, streamsegment = get_snapped_point_simple(
            conn, lon, lat, subc_id, basin_id, reg_id)

        # Extract snapped coordinates:
        snap_lon = snappedpoint["coordinates"][0]
        snap_lat = snappedpoint["coordinates"][1]

        # Make connecting line:
        connecting_line = {
            "type": "LineString",
            "coordinates":[[lon,lat],[snap_lon,snap_lat]]
        }

        # Get local subcatchment polygon too
        subcatchment_simple = get_polygon_for_subcid_simple(conn, subc_id, basin_id, reg_id)

        # Return only geometry:
        if geometry_only:
            geometry_coll = {
                "type": "GeometryCollection",
                "geometries": [snappedpoint, streamsegment, connecting_line]
            }

            if comment is not None:
                geometry_coll['comment'] = comment

            if self.return_hyperlink('snapped_point', requested_outputs):
                return 'application/json', self.store_to_json_file('snapped_point', geometry_coll)
            else:
                return 'application/json', geometry_coll


        # Return feature collection:
        if not geometry_only:

            # Make feature from snapped point:
            snappedpoint_feature = {
                "type": "Feature",
                "geometry": snappedpoint,
                "properties": {
                    "subcatchment_id": subc_id,
                    "basin_id": basin_id,
                    "reg_id": reg_id,
                    "lon_original": lon,
                    "lat_original": lat,
                }
            }

            # Make feature from snapped point:
            streamsegment_feature = {
                "type": "Feature",
                "geometry": streamsegment,
                "properties": {
                    "subcatchment_id": subc_id,
                    "basin_id": basin_id,
                    "reg_id": reg_id,
                    "strahler_order": strahler
                }
            }

            # Make feature from subcatchment polygon:
            subcatchment_feature = {
                "type": "Feature",
                "geometry": subcatchment_simple,
                "properties": {
                    "subcatchment_id": subc_id,
                    "basin_id": basin_id,
                    "reg_id": reg_id,
                    "strahler_order": strahler
                }
            }

            # Make feature from connecting line:
            connecting_line_feature = {
                "type": "Feature",
                "properties": {"description": "connecting line"},
                "geometry": connecting_line
            }

            # Create FeatureCollection:
            feature_coll = {
                "type": "FeatureCollection",
                "features": [snappedpoint_feature, streamsegment_feature, connecting_line_feature, subcatchment_feature]
            }

            if comment is not None:
                feature_coll['comment'] = comment

            if self.return_hyperlink('snapped_point', requested_outputs):
                return 'application/json', self.store_to_json_file('snapped_point', feature_coll)
            else:
                return 'application/json', feature_coll


    def return_hyperlink(self, output_name, requested_outputs):

        if requested_outputs is None:
            return False

        if 'transmissionMode' in requested_outputs.keys():
            if requested_outputs['transmissionMode'] == 'reference':
                return True

        if output_name in requested_outputs.keys():
            if 'transmissionMode' in requested_outputs[output_name]:
                if requested_outputs[output_name]['transmissionMode'] == 'reference':
                    return True

        return False


    def store_to_json_file(self, output_name, json_object):

        # Store to file
        downloadfilename = 'outputs-%s-%s.json' % (self.metadata['id'], self.job_id)
        downloadfilepath = self.config['download_dir']+downloadfilename
        LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
        with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
            json.dump(json_object, downloadfile, ensure_ascii=False, indent=4)

        # Create download link:
        downloadlink = self.config['download_url'] + downloadfilename

        # Create output to pass back to user
        outputs_dict = {
            'title': self.metadata['outputs'][output_name]['title'],
            'description': self.metadata['outputs'][output_name]['description'],
            'href': downloadlink
        }

        return outputs_dict


    def get_db_connection(self):

        config = self.config

        geofresh_server = config['geofresh_server']
        geofresh_port = config['geofresh_port']
        database_name = config['database_name']
        database_username = config['database_username']
        database_password = config['database_password']
        use_tunnel = config.get('use_tunnel')
        ssh_username = config.get('ssh_username')
        ssh_password = config.get('ssh_password')
        localhost = config.get('localhost')

        try:
            conn = get_connection_object(geofresh_server, geofresh_port,
                database_name, database_username, database_password,
                use_tunnel=use_tunnel, ssh_username=ssh_username, ssh_password=ssh_password)
        except sshtunnel.BaseSSHTunnelForwarderError as e1:
            LOGGER.error('SSH Tunnel Error: %s' % str(e1))
            raise e1

        return conn

