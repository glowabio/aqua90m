
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
import pygeoapi.process.aqua90m.geofresh.bbox as bbox
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a simple Geometry (Polygon) (just one, not a collection):
curl -X POST "http://localhost:5000/processes/get-upstream-bbox/execution" \
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
curl -X POST "http://localhost:5000/processes/get-upstream-bbox/execution" \
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


class UpstreamBboxGetter(BaseProcessor):

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
        return f'<UpstreamBboxGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the upstream bounding box..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        # Check for which outputs it is asking:
        if outputs is None:
            LOGGER.info('Client did not specify outputs, so all possible outputs are returned!')
            outputs = {'ALL': None}

        try:
            conn = get_connection_object_config(self.config)
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
        comment = data.get('comment', None) # optional
        add_upstream_ids = data.get('add_upstream_ids', 'false')
        geometry_only = data.get('geometry_only', 'false')

        # Parse booleans
        add_upstream_ids = (add_upstream_ids.lower() == 'true')
        geometry_only = (geometry_only.lower() == 'true')

        # Overall goal: Get the upstream stream segments!
        LOGGER.info('START: Getting upstream bbox for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

        # Get reg_id, basin_id, subc_id, upstream_ids
        subc_id, basin_id, reg_id = basic_queries.get_subc_id_basin_id_reg_id(
            conn, LOGGER, lon, lat, subc_id)
        upstream_ids = upstream_subcids.get_upstream_catchment_ids_incl_itself(
            conn, subc_id, basin_id, reg_id)

        if geometry_only:

            # Get bounding box:
            bbox_simplegeom = bbox.get_bbox_simplegeom(
                conn, upstream_ids, basin_id, reg_id)
            # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2

            if comment is not None:
                bbox_simplegeom['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('bbox', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('bbox', bbox_simplegeom,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', bbox_simplegeom

        if not geometry_only:

            # Get bounding box:
            # This geometry can be None/null, which is the valid value for unlocated Features in GeoJSON spec:
            # https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
            bbox_feature = bbox.get_bbox_feature(
                conn, upstream_ids, basin_id, reg_id, add_subc_ids = add_upstream_ids)


            # Add some info to the Feature:
            # TODO: Should we include the requested lon and lat? Maybe as a point? Then FeatureCollection?
            bbox_feature["description"] = "Bounding box of the upstream catchment of subcatchment %s" % subc_id
            bbox_feature["bbox_of_upstream_catchment_of"] = subc_id

            if comment is not None:
                bbox_feature['properties']['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('bbox', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('bbox', bbox_feature,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', bbox_feature

