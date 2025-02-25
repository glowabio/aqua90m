
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.aqua90m.geofresh.upstream_helpers import get_subc_id_basin_id_reg_id
from pygeoapi.process.aqua90m.geofresh.py_query_db import get_connection_object
import pygeoapi.process.aqua90m.geofresh.get_upstream_subcids as get_upstream_subcids
import pygeoapi.process.aqua90m.geofresh.get_dissolved_polygon as get_dissolved_polygon



'''
# Request a simple Geometry (Polygon) (just one, not a collection):
curl -X POST "http://localhost:5000/processes/get-upstream-dissolved/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "true",
    "comment": "schlei-bei-rabenholz"
    }
}'

# Request a Feature (Polygon) (just one, not a collection):
curl -X POST "http://localhost:5000/processes/get-upstream-dissolved/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "false",
    "add_upstream_ids": "true",
    "comment": "schlei-bei-rabenholz"
    }
}'

# Large: In the middle of river Elbe: 53.537158298376575, 9.99475350366553
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))

class UpstreamDissolvedGetter(BaseProcessor):

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
        return f'<UpstreamDissolvedGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream polygon..."')
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
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', 'false')
        add_upstream_ids = data.get('add_upstream_ids', 'true')

        # Parse booleans
        geometry_only = (geometry_only.lower() == 'true')
        add_upstream_ids = (add_upstream_ids.lower() == 'true')

        # Overall goal: Get the upstream polygon (as one dissolved)!
        LOGGER.info('START: Getting upstream dissolved polygon for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

        # Get reg_id, basin_id, subc_id
        subc_id, basin_id, reg_id = get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)

        # Get upstream id
        upstream_ids = get_upstream_subcids.get_upstream_catchment_ids_incl_itself(
            conn, subc_id, basin_id, reg_id)

        # Return only geometry:
        if geometry_only:

            dissolved_simplegeom = get_dissolved_polygon.get_dissolved_simplegeom(
                conn, upstream_ids, basin_id, reg_id)

            if comment is not None:
                dissolved_simplegeom['comment'] = comment

            if self.return_hyperlink('polygon', requested_outputs):
                return 'application/json', self.store_to_json_file('polygon', dissolved_simplegeom)
            else:
                return 'application/json', dissolved_simplegeom


        # Return Feature:
        if not geometry_only:

            dissolved_feature = get_dissolved_polygon.get_dissolved_feature(
                conn, upstream_ids, basin_id, reg_id, add_subc_ids = add_upstream_ids)

            # Add some info to Feature:
            # TODO: Should we include the requested lon and lat? Maybe as a point? Then FeatureCollection?
            dissolved_feature["description"] = "Dissolved upstream catchment of subcatchment %s" % subc_id
            dissolved_feature["dissolved_upstream_catchment_of"] = subc_id
            dissolved_feature["num_dissolved_sub_catchments"] = len(upstream_ids)

            if comment is not None:
                dissolved_feature['properties']['comment'] = comment

            if self.return_hyperlink('polygon', requested_outputs):
                return 'application/json', self.store_to_json_file('polygon', dissolved_feature)
            else:
                return 'application/json', dissolved_feature


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

