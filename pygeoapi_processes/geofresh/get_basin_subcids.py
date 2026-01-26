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
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''

# Request a list, based on basin_ids:
# Tested: 2026-01-26
curl -X POST https://$PYSERVER/processes/get-basin-subcids/execution \
--header "Content-Type: application/json" \
--data '{
    "inputs": {
        "basin_ids": [1293500],
        "min_strahler": 4,
        "comment": "something"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Request a list, based on subc_ids:
# Tested: 2026-01-26
curl -X POST https://$PYSERVER/processes/get-basin-subcids/execution \
--header "Content-Type: application/json" \
--data '{
    "inputs": {
        "subc_ids": [506319029, 509342352],
        "min_strahler": 4,
        "comment": "something"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'

# Request a list, based on points:
# Tested: 2026-01-26
curl -X POST https://$PYSERVER/processes/get-basin-subcids/execution \
--header "Content-Type: application/json" \
--data '{
    "inputs": {
        "points_geojson": {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [10.217977, 54.301799]
                },
                "properties": {}
            }]
        },
        "min_strahler": 6,
        "comment": "something"
    },
    "outputs": {
        "transmissionMode": "reference"
    }
}'
'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class BasinSubcidsGetter(GeoFreshBaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)


    def _execute(self, data, requested_outputs, conn):

        # User inputs
        # We need either basin_id OR lonlat/point OR subc_id
        # Input can be basins:
        basin_ids = data.get('basin_ids', None)
        # Or points, from which we infer the basin:
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url', None)
        # Or subc_ids, from which we infer the basin:
        subc_ids  = data.get('subc_ids',  None)
        # Deprecated:
        lon = data.get('lon', None)
        lat = data.get('lat', None)
        # Other params:
        min_strahler = data.get('min_strahler', None)
        comment = data.get('comment') # optional

        # Check presence:
        utils.at_least_one_param({
            "basin_ids": basin_ids,
            "subc_ids": subc_ids,
            "points_geojson": points_geojson,
            "points_geojson_url": points_geojson_url,
            "pair of coordinates (lon and lat)": (lon and lat)
        })

        ##############################
        ### Download if applicable ###
        ### and validate GeoJSON   ###
        ##############################

        if points_geojson_url is not None:
            points_geojson = utils.download_geojson(points_geojson_url)

        #########################################
        ### Prepare final list (to be filled) ###
        ### depending on input type           ###
        #########################################

        final_list = []

        ## If the user passed basin_ids directly, get reg_id for each basin,
        ## and add basin_id and reg_id to the list.
        if basin_ids is not None:

            LOGGER.debug(f'Retrieving reg_ids for {len(basin_ids)} basin_ids...')
            for basin_id in basin_ids:
                LOGGER.log(logging.TRACE, f'Retrieving reg_id for basin_id {basin_id}')
                reg_id = basic_queries.get_regid_from_basinid(conn, LOGGER, basin_id)
                final_list.append({
                    "basin_id": basin_id,
                    "reg_id": reg_id
                })

        ## If lonlat are given: Not supported anymore!
        elif lon is not None and lat is not None:
            raise NotImplementedError('Deprecated. Please use a GeoJSON point instead of lon and lat values.')

        ## If points are given
        elif points_geojson is not None:

            ## First, construct temporary dictionary:
            ##  {reg_id: {basin_id: [subc_ids]}}
            ## (because several points may be in one basin/region, and we
            ## don't want to query the database several times per basin).
            mydict = {}

            LOGGER.debug(f'Retrieving basin_ids and reg_ids for {len(points_geojson)} points...')
            points_geojson = points_geojson.get("features") or points_geojson.get("geometries")
            for point in points_geojson:

                # Retrieve basin_id and reg_id from database:
                LOGGER.log(logging.TRACE, f'Retrieving basin_ids for point {point}')
                lon, lat = point.get('coordinates') or point['geometry']['coordinates']
                subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, lon, lat)

                # Fill into temp dictionary:
                reg = str(reg_id)
                basin = str(basin_id)
                if reg not in mydict:
                    mydict[reg] = {}
                if basin not in mydict[reg]:
                    mydict[reg][basin] = []
                mydict[reg][basin].append(point)

            ## From the temp dict, make the list that will be the final result, once finished:
            basin_ids = []
            for reg in mydict.keys():
                reg_id = int(reg)
                for basin in mydict[reg].keys():
                    basin_id = int(basin)
                    basin_ids.append(basin_id)
                    final_list.append({
                        "basin_id": basin_id,
                        "reg_id": reg_id,
                        "input_points": mydict[reg][basin]
                    })

        ## If subc_ids are given:
        elif subc_ids is not None:

            ## First, construct temporary dictionary:
            ##  {reg_id: {basin_id: [subc_ids]}}
            ## (because several points may be in one basin/region, and we
            ## don't want to query the database several times per basin).
            mydict = {}

            LOGGER.debug(f'Retrieving basin_ids and reg_ids for {len(subc_ids)} subc_ids...')
            for subc_id in subc_ids:

                # Retrieve basin_id and reg_id from database:
                LOGGER.log(logging.TRACE, f'Retrieving basin_ids for subc_id {subc_id}')
                subc_id, basin_id, reg_id = basic_queries.get_subcid_basinid_regid(
                    conn, LOGGER, subc_id = subc_id)

                # Fill into temp dictionary:
                reg = str(reg_id)
                basin = str(basin_id)
                if reg not in mydict:
                    mydict[reg] = {}
                if basin not in mydict[reg]:
                    mydict[reg][basin] = []
                mydict[reg][basin].append(subc_id)

            ## From the temp dict, make the list that will be the final result, once finished:
            basin_ids = []
            for reg in mydict.keys():
                reg_id = int(reg)
                for basin in mydict[reg].keys():
                    basin_id = int(basin)
                    basin_ids.append(basin_id)
                    final_list.append({
                        "basin_id": basin_id,
                        "reg_id": reg_id,
                        "input_subc_ids": mydict[reg][basin]
                    })


        ########################
        ### Get all subc_ids ###
        ########################

        ## Iterate over the prepared list, for each item (i.e. each basin),
        ## get all subc_ids and append them to the item.
        for item in final_list:
            basin_id = item["basin_id"]
            reg_id = item["reg_id"]
            LOGGER.debug(f'Now, getting subc_ids for basin_id: {basin_id} (reg_id {reg_id}).')
            # TODO: This throws exceptions if basin has no subc_ids at that min_strahler!
            # Just return []...
            all_subcids = basic_queries.get_all_subcids_from_basinid(
                    conn, LOGGER, basin_id, reg_id, min_strahler=min_strahler)
            item["num_subcatchments"] = len(all_subcids)
            item["subc_ids"] = all_subcids


        # Note: This is not GeoJSON (on purpose), as we did not look for geometry:
        output_json = {
            "subcatchment_ids_per_basin": final_list,
            "basin_ids": basin_ids
        }
        if min_strahler is not None:
            output_json["min_strahler"] = min_strahler

        ## Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        return self.return_results('basin_subcatchment_ids', requested_outputs, output_json=output_json, comment=comment)


if __name__ == '__main__':

    import os
    import requests
    PYSERVER = f'https://{os.getenv("PYSERVER")}'
    # For this to work, please define the PYSERVER before running python:
    # export PYSERVER="https://.../pygeoapi-dev"
    print('_____________________________________________________')
    process_id = 'get-basin-subcids'
    print(f'TESTING {process_id} at {PYSERVER}')
    from pygeoapi.process.aqua90m.mapclient.test_requests import make_sync_request
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_basic
    from pygeoapi.process.aqua90m.mapclient.test_requests import sanity_checks_geojson


    print('TEST CASE 1: Input: basin_ids (in several regions), output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_ids": [1293500, 1173222, 1293023],
            "comment": "test1"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 2: Input: basin_id, min_strahler=7, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_ids": [1293500],
            "min_strahler": 7,
            "comment": "test2"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 3: Input: basin_id, min_strahler=8 (will fail), output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "basin_ids": [1293500],
            "min_strahler": 8,
            "comment": "test3"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        #print(f'RESP: {resp.json()}\n')
        print(f'TEST CASE 3: EXPECTED: {e.response.json()["description"]}')


    print('TEST CASE 4: Input: subc_id, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "subc_ids": [506319029, 506322510, 509342352, 513456602],
            "comment": "test4"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 5: Input: FeatureCollection (in several regions), output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [10.233422, 54.314711]
                        },
                        "properties": {
                            "strahler": 1,
                            "subc_id": 506319029,
                            "basin_id": 1293500,
                            "reg_id": 58,
                            "comment": "nordschwentine"
                        }
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [10.217977, 54.301799]
                        },
                        "properties": {
                            "strahler": 3,
                            "subc_id": 506322510,
                            "basin_id": 1293500,
                            "reg_id": 58,
                            "comment": "suedschwentine"
                        }
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [2.172944, 49.914233]
                        },
                        "properties": {
                            "strahler": 1,
                            "subc_id": 509342352,
                            "basin_id": 1173222,
                            "reg_id": 58,
                            "comment": "amiens"
                        }
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [12.398888, 43.082088]},
                        "properties": {
                            "strahler": 1,
                            "subc_id": 513456602,
                            "basin_id": 1293023,
                            "reg_id": 59,
                            "comment": "perugia"
                        }
                    }
                ]
            },
            "comment": "test5"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 6: Input: GeometryCollection (in several regions), output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson": {
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [10.233422, 54.314711]
                    },
                    {
                        "type": "Point",
                        "coordinates": [10.217977, 54.301799]
                    },
                    {
                        "type": "Point",
                        "coordinates": [2.172944, 49.914233]
                    },
                    {
                        "type": "Point",
                        "coordinates": [12.398888, 43.082088]
                    }
                ]
            },
            "comment": "test6"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')


    print('TEST CASE 7: Input: lon, lat, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "lon": 10.233422,
            "lat": 54.314711,
            "comment": "test7"
        }
    }
    try:
        resp = make_sync_request(PYSERVER, process_id, payload)
        raise ValueError("Expected error that did not happen...")
    except requests.exceptions.HTTPError as e:
        #print(f'RESP: {resp.json()}\n')
        print(f'TEST CASE 7: EXPECTED: {e.response.json()["description"]}')


    print('TEST CASE 8: Input: GeoJSON as URL, output: json...', end="", flush=True)  # no newline
    payload = {
        "inputs": {
            "points_geojson_url": "https://aqua.igb-berlin.de/referencedata/aqua90m/test_featurecollection_points.json",
            "comment": "test8"
        }
    }
    resp = make_sync_request(PYSERVER, process_id, payload)
    sanity_checks_basic(resp)
    #print(f'RESP: {resp.json()}\n')

