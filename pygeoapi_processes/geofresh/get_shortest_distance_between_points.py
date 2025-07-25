import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

TODO: This already provides the computation for just one pair of points, or for an entire
matrix. However, the results are quite different.


###########################################
### Simple:                             ###
### Request distance between two points ###
###########################################

# Input: Coordinate pairs
curl -X POST "http://localhost:5000/processes/get-shortest-distance-between-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon_start": 9.937520027160646,
    "lat_start": 54.69422745526058,
    "lon_end": 9.9217,
    "lat_end": 54.6917,
    "comment": "located in schlei area"
  }
}'

# Input: Subcatchment ids:
curl -X POST "http://localhost:5000/processes/get-shortest-distance-between-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_id_start": 506251713,
    "subc_id_end": 506251712,
    "comment": "located in schlei area"
  }
}'

############################################
### Matrix:                              ###
### Request distance between many points ###
############################################

# Input: Multipoint (one set of points)
curl -X POST "http://localhost:5000/processes/get-shortest-distance-between-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points": {
      "type": "MultiPoint",
      "coordinates": [
        [9.937520027160646, 54.69422745526058],
        [9.9217, 54.6917],
        [9.9312, 54.6933]
      ]
    },
    "comment": "located in schlei area"
  }
}'


# Input: Multipoint (two separate set of points)
curl -X POST "http://localhost:5000/processes/get-shortest-distance-between-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "points_start": {
      "type": "MultiPoint",
      "coordinates": [
        [9.9217, 54.6917],
        [9.9312, 54.6933]
      ]
    },
    "points_end": {
      "type": "MultiPoint",
      "coordinates": [
        [9.937520027160646, 54.69422745526058],
        [9.9217478273, 54.69173489023]
      ]
    },
    "comment": "located in schlei area"
  }
}'

# Input: One set of subc_ids
curl -X POST "http://localhost:5000/processes/get-shortest-distance-between-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_ids": [506251712, 506251713, 506252055],
    "comment": "located in schlei area"
  }
}'

# Input: Two sets of subc_ids
curl -X POST "http://localhost:5000/processes/get-shortest-distance-between-points/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_ids_start": [506251712, 506252055],
    "subc_ids_end": [506251712, 506251713],
    "comment": "located in schlei area"
  }
}'


'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class ShortestDistanceBetweenPointsGetter(BaseProcessor):

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
        return f'<ShortestDistanceBetweenPointsGetter> {self.name}'


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
            #TODO OR: raise ProcessorExecuteError(e, user_msg=e.message)


    def _execute(self, data, requested_outputs, conn):

        # Overall goal: Get the dijkstra distance!

        ####################
        ### User inputs: ###
        ####################

        # Two points:
        lon_start = data.get('lon_start', None)
        lat_start = data.get('lat_start', None)
        lon_end = data.get('lon_end', None)
        lat_end = data.get('lat_end', None)
        # Two subcatchments:
        subc_id_start = data.get('subc_id_start', None) # optional, need either lonlat OR subc_id
        subc_id_end = data.get('subc_id_end', None)     # optional, need either lonlat OR subc_id
        # Set of points (Multipoint):
        points = data.get('points', None)
        # Two separate sets of points (Multipoint):
        points_start = data.get('points_start', None)
        points_end = data.get('points_end', None)
        # Set of subcatchments:
        subc_ids = data.get('subc_ids', None)
        # Two separate sets of subcatchments:
        subc_ids_start = data.get('subc_ids_start', None)
        subc_ids_end = data.get('subc_ids_end', None)
        # Comment:
        comment = data.get('comment') # optional

        ############################
        ### Validate user inputs ###
        ############################

        if points is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of points (start and end points are the same)...')
        elif lon_start is not None and lat_start is not None and lon_end is not None and lat_end is not None:
            # TODO: Just ask users for two GeoJSON points?!?!
            LOGGER.debug('START: Getting dijkstra shortest distance between two of points...')
        elif subc_id_start is not None and subc_id_end is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between two subcatchments...')
        elif points_start is not None and points_end is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of points (start and end points are different)...')
        elif subc_ids is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of subcatchments (start and end points are the same)...')
        elif subc_ids_start is not None and subc_ids_end is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of subcatchments (start and end points are different)...')
        else:
            err_msg = 'You must specify either "point" or lon and lat of start and end point...'
            raise ProcessorExecuteError(err_msg)


        ############################
        ### Many points          ###
        ### starts and ends same ###
        ############################
        if points is not None or subc_ids is not None:

            if subc_ids is not None:
                all_subc_ids = set(subc_ids)

                # Should we also get all basin ids, reg ids?
                # Or just one ... ?
                basin_id, reg_id = basic_queries.get_basinid_regid_from_subcid(conn, LOGGER, subc_ids[0])
                #for subc_id in all_subc_ids:
                #    basin_id, reg_id = basic_queries.get_basinid_regid_from_subcid(conn, LOGGER, subc_id)
                #    all_reg_ids.add(reg_id)
                #    all_basin_ids.add(basin_id)

            elif points is not None:
                # Collect reg_id, basin_id, subc_id
                all_subc_ids = set()
                all_reg_ids = set()
                all_basin_ids = set()
                for lon, lat in points['coordinates']: # TODO: Maybe not do this loop based?
                    LOGGER.debug('Now getting subc_id, basin_id, reg_id for lon %s, lat %s' % (lon, lat))
                    subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                        conn, LOGGER, lon, lat)
                    all_subc_ids.add(subc_id)
                    all_reg_ids.add(reg_id)
                    all_basin_ids.add(basin_id)

                # Check if same region and basin?
                # TODO: Can we route via the sea then??
                if len(all_reg_ids) == 1:
                    reg_id = next(iter(all_reg_ids))
                else:
                    err_msg = 'The input points are in different regions (%s) - this cannot work.' % all_reg_ids
                    LOGGER.warning(err_msg)
                    raise ProcessorExecuteError(user_msg=err_msg)

                if len(all_basin_ids) == 1:
                    basin_id = next(iter(all_basin_ids))
                else:
                    err_msg = 'The input points are in different basins (%s) - this cannot work.' % all_basin_ids
                    LOGGER.warning(err_msg)
                    raise ProcessorExecuteError(user_msg=err_msg)

            # Get distance - this is a JSON-ified matrix:
            # (Complete matrix, starts and ends are the same set!)
            json_result = routing.get_dijkstra_distance_many(
                conn, all_subc_ids, all_subc_ids, reg_id, basin_id)

        ##################################
        ### Many points                ###
        ### starts and ends separately ###
        ##################################
        elif ((points_start is not None and points_end is not None) or 
            (subc_ids_start is not None and subc_ids_end is not None)):
            # This is basically the same as above!!

            if subc_ids_start is not None and subc_ids_end is not None:
                all_subc_ids_start = set(subc_ids_start)
                all_subc_ids_end = set(subc_ids_end)
                # Should we also get all basin ids, reg ids?
                # Or just one ... ?
                basin_id, reg_id = basic_queries.get_basinid_regid_from_subcid(conn, LOGGER, subc_ids_start[0])
                #for subc_id in all_subc_ids_start:
                #    basin_id, reg_id = get_basinid_regid_from_subcid(conn, LOGGER, subc_id)
                #    all_reg_ids_start.add(reg_id)
                #    all_basin_ids_start.add(basin_id)
                # same for end...

            elif points_start is not None and points_end is not None:
                # Collect reg_id, basin_id, subc_id
                # TODO: Make this a function?! We do this 3 times...
                all_subc_ids_start = set()
                all_reg_ids_start = set()
                all_basin_ids_start = set()
                for lon, lat in points_start['coordinates']: # TODO: Maybe not do this loop based?
                    LOGGER.debug('Now getting subc_id, basin_id, reg_id for lon %s, lat %s' % (lon, lat))
                    subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                        conn, LOGGER, lon, lat)
                    all_subc_ids_start.add(subc_id)
                    all_reg_ids_start.add(reg_id)
                    all_basin_ids_start.add(basin_id)

                all_subc_ids_end = set()
                all_reg_ids_end = set()
                all_basin_ids_end = set()
                for lon, lat in points_end['coordinates']: # TODO: Maybe not do this loop based?
                    LOGGER.debug('Now getting subc_id, basin_id, reg_id for lon %s, lat %s' % (lon, lat))
                    subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                        conn, LOGGER, lon, lat)
                    all_subc_ids_end.add(subc_id)
                    all_reg_ids_end.add(reg_id)
                    all_basin_ids_end.add(basin_id)

                # Check if same region and basin?
                # TODO: Can we route via the sea then??
                if len(all_reg_ids_start | all_reg_ids_end) == 1:
                    reg_id = all_reg_ids_start.pop()
                else:
                    err_msg = 'The input points are in different regions (%s) - this cannot work.' % all_reg_ids_start | all_reg_ids_end
                    LOGGER.warning(err_msg)
                    raise ProcessorExecuteError(user_msg=err_msg)

                if len(all_basin_ids_start | all_basin_ids_end) == 1:
                    basin_id = all_basin_ids_start.pop()
                else:
                    err_msg = 'The input points are in different basins (%s) - this cannot work.' % all_basin_ids_start | all_basin_ids_end
                    LOGGER.warning(err_msg)
                    raise ProcessorExecuteError(user_msg=err_msg)

            # Get distance - this is a JSON-ified matrix:
            # (Complete matrix, starts and ends are the same set!)
            json_result = routing.get_dijkstra_distance_many(
                conn, all_subc_ids_start, all_subc_ids_end, reg_id, basin_id)


        #################
        ### One point ###
        #################
        else:

            if lon_start is not None and lat_start is not None and lon_end is not None and lat_end is not None:

                # Get reg_id, basin_id, subc_id
                # Point 1:
                subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon_start, lat_start)
                # Point 2:
                subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon_end, lat_end)

            # Special case: user provided subc_id instead of lonlat!
            elif subc_id_start is not None and subc_id_end is not None:

                # Get reg_id, basin_id, subc_id
                # Point 1:
                subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, subc_id = subc_id_start)
                # Point 2:
                subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, subc_id = subc_id_end)

            else:
                err_msg = 'Please check your inputs. Could not identify start and end point. Expected either coordinate pairs or two subc_ids.'
                LOGGER.warning(err_msg)
                raise ProcessorExecuteError(user_msg=err_msg)

            # Check if same region and basin?
            # TODO: Can we route via the sea then??
            if not reg_id1 == reg_id2:
                err_msg = 'Start and end are in different regions (%s and %s) - this cannot work.' % (reg_id1, reg_id2)
                LOGGER.warning(err_msg)
                raise ProcessorExecuteError(user_msg=err_msg)

            if not basin_id1 == basin_id2:
                err_msg = 'Start and end are in different basins (%s and %s) - this cannot work.' % (basin_id1, basin_id2)
                LOGGER.warning(err_msg)
                raise ProcessorExecuteError(user_msg=err_msg)

            # Get distance - just a number:
            dist = routing.get_dijkstra_distance_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)
            json_result = {
                "distance": dist,
                "from": subc_id1,
                "to": subc_id2,
                "basin_id": basin_id1,
                "reg_id": reg_id1
            }
            # TODO: Like this, the output is quite different between the two!!


        # For both:
        if comment is not None:
            json_result['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        if utils.return_hyperlink('distances_matrix', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('distances_matrix', json_result,
                self.metadata, self.job_id,
                    self.download_dir,
                    self.download_url)
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', json_result
