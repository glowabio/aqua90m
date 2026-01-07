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
from pygeoapi.process.aqua90m.pygeoapi_processes.geofresh.GeoFreshBaseProcessor import GeoFreshBaseProcessor
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.geofresh.distances as distances
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
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-distance-between-points/execution \
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
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-distance-between-points/execution \
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
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-distance-between-points/execution \
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
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-distance-between-points/execution \
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
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-distance-between-points/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "subc_ids": [506251712, 506251713, 506252055],
    "comment": "located in schlei area"
  }
}'

# Input: Two sets of subc_ids
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-distance-between-points/execution \
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


class ShortestDistanceBetweenPointsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def _execute(self, data, requested_outputs, conn):

        # Overall goal: Get the dijkstra distance!

        ####################
        ### User inputs: ###
        ####################

        # Singular case:
        # Two points:
        lon_start = data.get('lon_start', None)
        lat_start = data.get('lat_start', None)
        lon_end = data.get('lon_end', None)
        lat_end = data.get('lat_end', None)
        # Two subcatchments:
        subc_id_start = data.get('subc_id_start', None) # optional, need either lonlat OR subc_id
        subc_id_end = data.get('subc_id_end', None)     # optional, need either lonlat OR subc_id

        # Plural case:
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

        # Output format (can be csv or json):
        result_format = data.get('result_format', 'json')
        # Comment:
        comment = data.get('comment') # optional

        ###########################
        ### Plural or singular? ###
        ###########################

        # Singular or plural case?
        singular = False
        plural_symmetric = False
        plural_asymmetric = False
        
        # Which inputs 
        if not (lon_start is None
            and lat_start is None
            and lon_end is None
            and lat_end is None
            and subc_id_start is None
            and subc_id_end is None
            ):
            LOGGER.debug('Singular case...')
            singular = True
        elif not(points is None
             and points_start is None
             and points_end is None
             and subc_ids is None
             and subc_ids_start is None
             and subc_ids_end is None
            ):
            LOGGER.debug('Plural case...')
        else:
            err_msg = 'You must specify start and end point(s) as point(s) or subc_id(s).'
            raise ProcessorExecuteError(err_msg)

        # TODO: Like this, the output is quite different between singular and plural!!
        if singular:
            return self.singular_case(conn, lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end, requested_outputs, comment)
        else:
            return self.plural_case(conn, points, points_start, points_end, subc_ids, subc_ids_start, subc_ids_end, requested_outputs, comment, result_format)


    def singular_case(self, conn, lon_start, lat_start, subc_id_start, lon_end, lat_end, subc_id_end, requested_outputs, comment):

        # Check if either subc_id or both lon and lat are provided:
        utils.params_lonlat_or_subcid(lon_start, lat_start, subc_id_start)
        utils.params_lonlat_or_subcid(lon_end,   lat_end,   subc_id_end)

        # Potential outputs
        json_result = None

        # First, get the ids required for querying the database efficiently:
        if lon_start is not None and lat_start is not None and lon_end is not None and lat_end is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between two points...')

            # Get reg_id, basin_id, subc_id
            # Point 1:
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon_start, lat_start)
            # Point 2:
            subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon_end, lat_end)

        elif subc_id_start is not None and subc_id_end is not None:
            # Special case: user provided subc_id instead of lonlat!
            LOGGER.debug('START: Getting dijkstra shortest distance between two subcatchments...')

            # Get reg_id, basin_id, subc_id
            # Point 1:
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id_start)
            # Point 2:
            subc_id2, basin_id2, reg_id2 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id_end)

        # Check if same region and basin?
        # TODO: Can we route via the sea then??
        if not reg_id1 == reg_id2:
            err_msg = f'Start and end are in different regions ({reg_id1} and {reg_id2}) - this cannot work.'
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        if not basin_id1 == basin_id2:
            err_msg = f'Start and end are in different basins ({basin_id1} and {basin_id2}) - this cannot work.'
            LOGGER.warning(err_msg)
            raise ProcessorExecuteError(user_msg=err_msg)

        # Get distance - just a number:
        dist = distances.get_dijkstra_distance_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)
        json_result = {
            "distance": dist,
            "from": subc_id1,
            "to": subc_id2,
            "basin_id": basin_id1,
            "reg_id": reg_id1
        }

        return self.return_results('distances_matrix', requested_outputs, output_df=None, output_json=json_result, comment=comment)


    def plural_case(self, conn, points, points_start, points_end, subc_ids, subc_ids_start, subc_ids_end, requested_outputs, comment, result_format):

        # Symmetric or asymmetric matrix? I.e. are the start and end points
        # the same, or different sets?
        plural_symmetric = plural_asymmetric = False
        if not(points is None and subc_ids is None):
            LOGGER.debug('Plural case, symmetric matrix...')
            plural_symmetric = True
        elif not (subc_ids_start is None
              and subc_ids_end is None
              and points_start is None
              and points_end is None
            ):
            LOGGER.debug('Plural case, asymmetric matrix...')
            plural_asymmetric = True

        # Get sets of input points
        if plural_symmetric:
            all_subc_ids_start, all_subc_ids_end, reg_id, basin_id = self.plural_symmetric(conn, points, subc_ids)
        elif plural_asymmetric:
            all_subc_ids_start, all_subc_ids_end, reg_id, basin_id = self.plural_asymmetric(conn, points_start, points_end, subc_ids_start, subc_ids_end)

        # Get distance:
        if result_format == "csv":
            output_df = distances.get_dijkstra_distance_many(
                conn, all_subc_ids_start, all_subc_ids_end, reg_id, basin_id, "dataframe")
            return self.return_results('distances_matrix', requested_outputs, output_df=output_df, comment=comment)
        else:
            # As a JSON-ified matrix:
            json_result = distances.get_dijkstra_distance_many(
                conn, all_subc_ids_start, all_subc_ids_end, reg_id, basin_id, "json")
            return self.return_results('distances_matrix', requested_outputs, output_json=json_result, comment=comment)


    def plural_symmetric(self, conn, points, subc_ids):

        if subc_ids is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of subcatchments (start and end points are the same)...')
            all_subc_ids = set(subc_ids)

            # Should we also get all basin ids, reg ids?
            # Or just one ... ?
            basin_id, reg_id = basic_queries.get_basinid_regid_from_subcid(conn, LOGGER, subc_ids[0])
            #for subc_id in all_subc_ids:
            #    basin_id, reg_id = basic_queries.get_basinid_regid_from_subcid(conn, LOGGER, subc_id)
            #    all_reg_ids.add(reg_id)
            #    all_basin_ids.add(basin_id)

        elif points is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of points (start and end points are the same)...')
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

        # Return...
        return all_subc_ids, all_subc_ids, reg_id, basin_id


    def plural_asymmetric(self, conn, points_start, points_end, subc_ids_start, subc_ids_end):

        if subc_ids_start is not None and subc_ids_end is not None:
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of subcatchments (start and end points are different)...')

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
            LOGGER.debug('START: Getting dijkstra shortest distance between a number of points (start and end points are different)...')

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

        # Return...
        return all_subc_ids_start, all_subc_ids_end, reg_id, basin_id





if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-shortest-distance-between-points'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson

    ###########################################
    ### Simple:                             ###
    ### Request distance between two points ###
    ###########################################

    print('TEST CASE 1: Simple: Request distance between two points, based on lonlat...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon_start": 9.937520027160646,
            "lat_start": 54.69422745526058,
            "lon_end": 9.9217,
            "lat_end": 54.6917,
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 2: Simple: Request distance between two points, based on subc_ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_id_start": 506251713,
            "subc_id_end": 506251712,
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    ############################################
    ### Matrix:                              ###
    ### Request distance between many points ###
    ############################################

    print('TEST CASE 3: Matrix: Request distances between many points. Input GeoJSON directly (Geometry: MultiPoint)...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points": {
                "type": "MultiPoint",
                "coordinates": [
                    [9.937520027160646, 54.69422745526058],
                    [9.9217, 54.6917],
                    [9.9312, 54.6933]
                ]
            },
            "comment": "test3"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 4: Matrix: Request distances between two sets of points. Input GeoJSON directly (Geometry: MultiPoint)...', end="", flush=True)  # no newline
    payload = {
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
            "comment": "test4"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 5: Matrix: Request distances between many points, based on subc_ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_ids": [506251712, 506251713, 506252055],
            "comment": "test5"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)


    print('TEST CASE 6: Matrix: Request distances between two sets of points, based on subc_ids...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_ids_start": [506251712, 506252055],
            "subc_ids_end": [506251712, 506251713],
            "comment": "test6"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
