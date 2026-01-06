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
import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
import pygeoapi.process.aqua90m.geofresh.dissolved as dissolved
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a URL to simple Geometry (Polygon) (just one, not a collection):
curl -X POST https://${PYSERVER}/processes/get-upstream-dissolved/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "get_type": "polygon",
    "comment": "schlei-near-rabenholz"
    }
}'

# Request (directly) a simple Geometry (Polygon) (just one, not a collection):
curl -X POST https://${PYSERVER}/processes/get-upstream-dissolved/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "get_type": "polygon",
    "get_json_directly": "true",
    "comment": "schlei-bei-rabenholz"
    }
}'

# Request a URL to Feature (Polygon) (just one, not a collection):
curl -X POST https://${PYSERVER}/processes/get-upstream-dissolved/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "get_type": "feature",
    "comment": "schlei-bei-rabenholz"
    }
}'

# Request a URL to FeatureCollection (Polygon):
curl -X POST https://${PYSERVER}/processes/get-upstream-dissolved/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "get_type": "featurecollection",
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

class UpstreamDissolvedGetter(GeoFreshBaseProcessor):


    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # TODO: Must change behaviour based on content of requested_outputs.
        # So far, I ignore them...
        LOGGER.debug(f'Content of requested_outputs: {requested_outputs}')

        # User inputs
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        comment = data.get('comment') # optional
        get_type = data.get('get_type', 'polygon')
        get_json_directly = data.get('get_json_directly', 'false') # Default: Return URL!
        subc_id = None # Needed below...

        # Parse booleans...  TODO: AIP Legacy, in future these should be booleans directly!
        get_json_directly = (get_json_directly.lower() == 'true')

        # Check if either subc_id or both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon, lat, subc_id)

        # Check types:
        utils.check_type_parameter('get_type', get_type, str)
        utils.is_bool_parameters(dict(get_json_directly=get_json_directly))

        # Overall goal: Get the upstream polygon (as one dissolved)!
        LOGGER.info(f'START PROCESS: Getting upstream dissolved polygon for lon, lat: {lon}, {lat} (or subc_id {subc_id})')

        # Get reg_id, basin_id, subc_id
        if subc_id is not None:
            # (special case: user provided subc_id instead of lonlat!)
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        else:
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)

        # Get upstream_catchment_ids
        LOGGER.debug(f'Requesting upstream catchment ids for subc_id: {subc_id}')
        upstream_catchment_ids = upstream_subcids.get_upstream_catchment_ids_incl_itself(
            conn, subc_id, basin_id, reg_id)

        # Get geometry (three types)
        LOGGER.debug(f'Requesting dissolved polygon of upstream catchment for subc_id: {subc_id}')
        geojson_object = {}
        if get_type.lower() == 'polygon':
            geojson_object = dissolved.get_dissolved_simplegeom(
                conn, upstream_catchment_ids, basin_id, reg_id)
            LOGGER.debug('END: Received simple polygon : %s' % str(geojson_object)[0:50])

        elif get_type.lower() == 'feature':
            geojson_object = dissolved.get_dissolved_feature(
                conn, upstream_catchment_ids, basin_id, reg_id, add_subc_ids = False)
            if comment is not None:
                geojson_object["properties"]["comment"] = comment
            LOGGER.debug('END: Received feature : %s' % str(geojson_object)[0:50])
       
        elif get_type.lower() == 'featurecollection':
            dissolved_feature = dissolved.get_dissolved_feature(
                conn, upstream_catchment_ids, basin_id, reg_id, add_subc_ids = False)

            # Create point:
            point_feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "subc_id": subc_id
                }
            }

            # Assemble GeoJSON Feature Collection
            # (point and dissolved upstream catchment):
            geojson_object = {
                "type": "FeatureCollection",
                "features": [dissolved_feature, point_feature],
            }
            if comment is not None:
                geojson_object["comment"] = comment
            LOGGER.debug('END: Received feature collection: %s' % str(geojson_object)[0:50])

        else:
            err_msg = "Input parameter 'get_type' can only be one of Polygon or Feature or FeatureCollection!"
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)



        ################
        ### Results: ###
        ################

        outputs_dict = {}

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
            downloadfilepath = self.download_dir+downloadfilename
            LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
            with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
                json.dump(geojson_object, downloadfile, ensure_ascii=False, indent=4)

            # Create download link:
            downloadlink = self.download_url + downloadfilename

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


