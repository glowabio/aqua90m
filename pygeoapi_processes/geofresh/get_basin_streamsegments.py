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
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

# Request a FeatureCollection, based on a basin_id:
# Output: LineStrings (FeatureCollection)
curl -X POST "https://$PYSERVER/processes/get-basin-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": false,
    "comment": "close to bremerhaven",
    "strahler_min": 4,
    "add_segment_ids": true
    }
}'

# Request a simple GeometryCollection, based on a basin_id
# Output: LineStrings (GeometryCollection)
curl -X POST "https://$PYSERVER/processes/get-basin-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "basin_id": 1288419,
    "geometry_only": true,
    "comment": "close to bremerhaven"
    }
}'

# Request a simple GeometryCollection, based on a subc_id
# Output: LineStrings (GeometryCollection)
curl -X POST "https://$PYSERVER/processes/get-basin-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_id": 506586041,
    "geometry_only": true,
    "comment": "close to bremerhaven"
    }
}'


# Request a simple GeometryCollection, based on lon+lat
# Output: LineStrings (GeometryCollection)
curl -X POST "https://$PYSERVER/processes/get-basin-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 8.278198242187502,
    "lat": 53.54910661890981,
    "geometry_only": true
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class BasinStreamSegmentsGetter(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None
        self.config = None

        # Set config:
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            self.config = json.load(config_file)
            self.download_dir = self.config['download_dir']
            self.download_url = self.config['download_url']


    def set_job_id(self, job_id: str):
        self.job_id = job_id


    def __repr__(self):
        return f'<BasinStreamSegmentsGetter> {self.name}'


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
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        subc_id  = data.get('subc_id',  None) # optional, need either lonlat OR subc_id
        basin_id = data.get('basin_id', None) # optional, need either lonlat OR subc_id
        strahler_min = data.get('strahler_min', 0)
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', False)
        add_segment_ids = data.get('add_segment_ids', False)

        # Check type:
        if not type(geometry_only) == bool:
            err_msg = f'Parameter "geometry_only" should be a boolean instead of a "{type(geometry_only)}".'

        # Get reg_id, basin_id, subc_id
        if subc_id is not None:
            LOGGER.info('Retrieving basin_id for subc_id %s' % subc_id)
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id)
        elif lon is not None and lat is not None:
            LOGGER.info('Retrieving basin_id for lon, lat: %s, %s' % (lon, lat))
            subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon, lat)
        elif basin_id is not None:
            reg_id = basic_queries.get_regid_from_basinid(conn, LOGGER, basin_id)
        else:
            err_msg = "Missing input. Need either basin_id, subc_id, or lon+lat."
            LOGGER.warn(err_msg)
            raise ProcessorExecuteError(err_msg)


        # Get only geometry:
        if geometry_only:

            LOGGER.debug('Now, getting stream segments for basin_id: %s' % basin_id)
            geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll_by_basin(
                conn, basin_id, reg_id, strahler_min = strahler_min)
        
            if comment is not None:
                geometry_coll['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('stream_segments', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('stream_segments', geometry_coll,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', geometry_coll


        # Get Feature:
        if not geometry_only:

            LOGGER.debug('Now, getting stream segments for basin_id: %s' % basin_id)
            feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll_by_basin(
                conn, basin_id, reg_id, strahler_min = strahler_min)

            if comment is not None:
                feature_coll['comment'] = comment

            if add_segment_ids:
                segment_ids = []
                for item in feature_coll['features']:
                    segment_ids.append(item["properties"]["subc_id"])
                feature_coll['segment_ids'] = segment_ids


            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('stream_segments', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('stream_segments', feature_coll,
                    self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', feature_coll


