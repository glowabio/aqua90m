
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.utils.upstream_helpers as helpers
from pygeoapi.process.geofresh.py_query_db import get_connection_object
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_feature
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_geometry
from pygeoapi.process.geofresh.py_query_db import get_upstream_catchment_dissolved_feature_coll




'''
# Small:
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-upstream-dissolved/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.931555, \"lat\": 54.695070, \"get_type\":\"Feature\",  \"comment\":\"Nordoestliche Schlei bei Rabenholz\"}}"

# Large: Mitten in der Elbe: 53.537158298376575, 9.99475350366553
curl -X POST "https://aqua.igb-berlin.de/pygeoapi/processes/get-upstream-dissolved/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lon\": 9.994753, \"lat\": 53.537158, \"comment\":\"Mitten inner Elbe bei Hamburg\"}}"
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))

class UpstreamDissolvedGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True # Maybe before super() ?
        self.job_id = None
        # To support requested outputs, such as transmissionMode
        # https://github.com/geopython/pygeoapi/blob/fef8df120ec52121236be0c07022490803a47b92/pygeoapi/process/manager/base.py#L253


    def __repr__(self):
        return f'<UpstreamDissolvedGetter> {self.name}'


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream polygon..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Outputs: %s' % outputs)

        try:
            return self._execute(data, outputs)
        except Exception as e:
            LOGGER.error(e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e)

    def _execute(self, data, requested_outputs):

        # TODO: Must change behaviour based on content of requested_outputs
        LOGGER.debug('Content of requested_outputs: %s' % requested_outputs)

        # User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        comment = data.get('comment') # optional
        get_type = data.get('get_type', 'polygon')
        get_json_directly = data.get('get_json_directly', 'false') # Default: Return URL!
        subc_id = None # Needed below...

        # Parse booleans...
        get_json_directly = (get_json_directly.lower() == 'true')

        # Get config
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)

        geofresh_server = config['geofresh_server']
        geofresh_port = config['geofresh_port']
        database_name = config['database_name']
        database_username = config['database_username']
        database_password = config['database_password']
        use_tunnel = config.get('use_tunnel')
        ssh_username = config.get('ssh_username')
        ssh_password = config.get('ssh_password')
        localhost = config.get('localhost')

        error_message = None

        try:
            conn = get_connection_object(geofresh_server, geofresh_port,
                database_name, database_username, database_password,
                use_tunnel=use_tunnel, ssh_username=ssh_username, ssh_password=ssh_password)
        except sshtunnel.BaseSSHTunnelForwarderError as e1:
            error_message = str(e1)

        try:
            # Overall goal: Get the upstream polygon (as one dissolved)!
            LOGGER.info('START: Getting upstream dissolved polygon for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

            # Get reg_id, basin_id, subc_id, upstream_catchment_ids
            subc_id, basin_id, reg_id = helpers.get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, subc_id)
            upstream_catchment_ids = helpers.get_upstream_catchment_ids(conn, subc_id, basin_id, reg_id, LOGGER)

            # Get geometry (three types)
            LOGGER.debug('...Getting upstream catchment dissolved polygon for subc_id: %s' % subc_id)
            geojson_object = {}
            if get_type.lower() == 'polygon':
                geojson_object = get_upstream_catchment_dissolved_geometry(
                    conn, subc_id, upstream_catchment_ids, basin_id, reg_id)
                LOGGER.debug('END: Received simple polygon : %s' % str(geojson_object)[0:50])

            elif get_type.lower() == 'feature':
                geojson_object = get_upstream_catchment_dissolved_feature(
                    conn, subc_id, upstream_catchment_ids,
                    basin_id, reg_id, comment=comment)
                LOGGER.debug('END: Received feature : %s' % str(geojson_object)[0:50])
           
            elif get_type.lower() == 'featurecollection':
                geojson_object = get_upstream_catchment_dissolved_feature_coll(
                    conn, subc_id, upstream_catchment_ids, (lon, lat),
                    basin_id, reg_id, comment=comment)
                LOGGER.debug('END: Received feature collection: %s' % str(geojson_object)[0:50])

            else:
                err_msg = "Input parameter 'get_type' can only be one of Polygon or Feature or FeatureCollection!"
                LOGGER.error(err_msg)
                raise ProcessorExecuteError(user_msg=err_msg)

                
        except ValueError as e2:
            error_message = str(e2)
            conn.close()
            raise ValueError(e2)

        except psycopg2.Error as e3:
            err = f"{type(e3).__module__.removesuffix('.errors')}:{type(e3).__name__}: {str(e3).rstrip()}"
            LOGGER.error(err)
            error_message = str(e3)
            error_message = str(err)
            error_message = 'Database error. '
            #if conn: conn.rollback()


        LOGGER.debug('Closing connection...')
        conn.close()
        LOGGER.debug('Closing connection... Done.')


        ################
        ### Results: ###
        ################

        if error_message is None:
            outputs_dict = {}

            if comment is not None: # TODO this is double!
                geojson_object['comment'] = comment

            # If the client requests a URL, we store it to file and pass the href:
            # This part is implemented to enable the AIP.
            #
            # The code is based on commit e74d1e2, "First attempt at considering requested_outputs in return behaviour",
            # but then I noticed that I treat the requested_outputs fundamentally wrong.
            # This code here is now an attempt to provide the AIP with a version that does not
            # change its behaviour, but that is not fundamentally wrong about requested_outputs.
            # The other get_output_dissolved.py version may evolve, which may disrupt the Beta AIP.
            if not get_json_directly:
                LOGGER.debug('Client requested an URL in the response.')

                # Store file
                downloadfilename = 'polygon-%s.json' % self.job_id
                #downloadfilepath = '/var/www/nginx/download'+os.sep+downloadfilename
                downloadfilepath = config['download_dir']+downloadfilename
                LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
                with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
                    json.dump(geojson_object, downloadfile, ensure_ascii=False, indent=4)

                # Create download link:
                #downloadlink = 'https://aqua.igb-berlin.de/download/'+downloadfilename
                downloadlink = config['download_url'] + downloadfilename

                # Build response containing the link
                output_name = 'polygon'
                response_object = {
                    "outputs": {
                        "polygon": {
                        'title': self.metadata['outputs'][output_name]['title'],
                        'description': self.metadata['outputs'][output_name]['description'],
                            "href": downloadlink
                        }
                    }
                }
                LOGGER.debug('Built response including link: %s' % response_object)
                return 'application/json', response_object

            else: # If the client explicitly requests JSON!
                LOGGER.debug('Client requested JSON response. Returning GeoJSON directly.')
                return 'application/json', geojson_object


        else:
            output = { # TODO check syntax here!
                'error_message': 'getting upstream polygon (dissolved) failed.',
                'details': error_message}

            if comment is not None:
                output['comment'] = comment

            LOGGER.warning('Getting upstream polygon (dissolved) failed. Returning error message.')
            return 'application/json', output

