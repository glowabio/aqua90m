
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.upstream_subcids as upstream_subcids
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config

'''

# Request a GeometryCollection (LineStrings):
curl -X POST "http://localhost:5000/processes/get-upstream-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "true",
    "comment": "schlei-near-rabenholz"
    }
}'

# Request a FeatureCollection (LineStrings):
curl -X POST "http://localhost:5000/processes/get-upstream-streamsegments/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.931555,
    "lat": 54.695070,
    "geometry_only": "false",
    "add_upstream_ids": "true",
    "comment": "schlei-near-rabenholz"
    }
}'

# Large: In the middle of Elbe river: 53.537158298376575, 9.99475350366553

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class UpstreamStreamSegmentsGetter(BaseProcessor):

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
        return f'<UpstreamStreamSegmentsGetter> {self.name}'


    def execute(self, data, outputs):
        LOGGER.info('Starting to get the upstream stream segments..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

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
        comment = data.get('comment') # optional
        add_upstream_ids = data.get('add_upstream_ids', 'false')
        geometry_only = data.get('geometry_only', 'false')

        # Parse add_upstream_ids
        geometry_only = (geometry_only.lower() == 'true')
        add_upstream_ids = (add_upstream_ids.lower() == 'true')

        # Overall goal: Get the upstream stream segments
        LOGGER.info('Getting upstream line segments for lon, lat: %s, %s (or subc_id %s)' % (lon, lat, subc_id))

        # Get reg_id, basin_id, subc_id, upstream_ids
        subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
            conn, LOGGER, lon, lat, subc_id)
        upstream_ids = upstream_subcids.get_upstream_catchment_ids_incl_itself(
            conn, subc_id, basin_id, reg_id)

        # Cumulative length as JSON:
        # TODO: We could include this into the query for the FeatureCollection,
        # instead of querying the database twice. But for now, it works.
        LOGGER.debug("Querying for cumulative length...")
        cum_length_by_strahler = get_linestrings.get_accum_length_by_strahler(
            conn, upstream_ids, basin_id, reg_id)
        LOGGER.debug("Querying for cumulative length DONE: %s" % cum_length_by_strahler)

        # Log interesting cases:
        if len(upstream_ids) == 0:
            LOGGER.warning('No upstream ids. Cannot get upstream linestrings .')
        if len(upstream_ids) == 1 and subc_id == upstream_ids[0]:
            LOGGER.debug('Upstream catchments equals subcatchment!')


        # Get geometry only:
        if geometry_only:

            if len(upstream_ids) == 0:
                geometry_coll = {
                    "type": "GeometryCollection",
                    "geometries": []
                }
            else:
                LOGGER.debug('... Getting upstream catchment line segments for subc_id: %s' % subc_id)
                geometry_coll = get_linestrings.get_streamsegment_linestrings_geometry_coll(conn, upstream_ids, basin_id, reg_id)

            LOGGER.debug('END: Received GeometryCollection: %s' % str(geometry_coll)[0:50])

            if comment is not None:
                geometry_coll['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('upstream_stream_segments', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('polygons', geometry_coll,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', geometry_coll


        # Get FeatureCollection
        if not geometry_only:

            if len(upstream_ids) == 0:
                # Feature Collections can have empty array according to GeoJSON spec::
                # https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
                feature_coll = {
                    "type": "FeatureCollection",
                    "features": [],
                    "basin_id": basin_id,
                    "reg_id": reg_id,
                    "cumulative_length": 0,
                    "cumulative_length_by_strahler": 0
                }

            else:
                # Note: The feature collection contains the strahler order for each feature (each stream segment)
                LOGGER.debug('... Getting upstream catchment line segments for subc_id: %s' % subc_id)
                feature_coll = get_linestrings.get_streamsegment_linestrings_feature_coll(
                    conn, upstream_ids, basin_id, reg_id, add_subc_ids = add_upstream_ids)

            # Add some info to the FeatureCollection:
            feature_coll["part_of_upstream_catchment_of"] = subc_id
            feature_coll["cumulative_length"] = cum_length_by_strahler["all_strahler_orders"],
            feature_coll["cumulative_length_by_strahler"] = cum_length_by_strahler

            LOGGER.debug('END: Received FeatureCollection: %s' % str(feature_coll)[0:50])

            if comment is not None:
                feature_coll['comment'] = comment

            # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
            if utils.return_hyperlink('upstream_stream_segments', requested_outputs):
                output_dict_with_url =  utils.store_to_json_file('polygons', feature_coll,
                    self.metadata, self.job_id,
                    self.config['download_dir'],
                    self.config['download_url'])
                return 'application/json', output_dict_with_url
            else:
                return 'application/json', feature_coll
