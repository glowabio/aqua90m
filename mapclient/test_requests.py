'''
Test script that makes simple requests (synchronous and asynchronous)
to a list of pygeoapi processes, always with the same payload,
always checking whether the result is valid GeoJSON.

This is to check whether the requests made by the mapclient are
functioning.

'''

import requests
import geojson
from datetime import datetime
import os

PYSERVER = os.getenv('PYSERVER')
# For this to work, please define the PYSERVER before running python:
# export PYSERVER="myhost.de/pygeoapi-dev"

#################
### Constants ###
#################

RAISE_ERROR=True
PYSERVER = f'https://{PYSERVER}'

process_ids_one_pair = [
    "get-upstream-bbox",
    "get-upstream-dissolved-cont",
    "get-upstream-subcatchments",
    "get-upstream-streamsegments",
    "get-local-streamsegments",
    "get-local-streamsegments-subcatchments",
    "get-snapped-points",
    "get-snapped-point-plus",
    "get-shortest-path-to-outlet"
]
process_ids_two_pairs = [
    "get-shortest-path-between-points"
]

HEADERS_SYNC = {'Content-Type': 'application/json'}
HEADERS_ASYNC = {'Content-Type': 'application/json', 'Prefer': 'respond-async'}

lon1 = 9.931555
lat1 = 54.695070
lon2 = 9.931
lat2 = 54.695

PAYLOAD_ONE_PAIR = { "inputs": {
    "lon":lon1,
    "lat":lat1
}}

PAYLOAD_TWO_PAIRS = { "inputs": {
    "lon_start":lon1, "lat_start":lat1,
    "lon_end":  lon2, "lat_end":  lat2
}}


#################
### Functions ###
#################

def make_sync_request(pyserver, process_id, payload):

    # First and only request:
    url = f'{pyserver}/processes/{process_id}/execution'
    resp = requests.post(url, json=payload, headers=HEADERS_SYNC)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f' NOT OK: {e.response.json()["description"]}')
        if RAISE_ERROR: raise e

    # If we get another successful code than 200 (e.g. 201)
    if not resp.status_code == 200:
        msg = f' NOT OK: Responded with {resp.status_code} instead of 200.'
        print(msg)
        if RAISE_ERROR: raise ValueError(msg)
        return None
    return resp

def make_async_request(pyserver, process_id, payload):

    # First request
    url = f'{pyserver}/processes/{process_id}/execution'
    resp = requests.post(url, json=payload, headers=HEADERS_ASYNC)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f' NOT OK: {e.response.json()["description"]}')
        if RAISE_ERROR: raise e

    # If we get another successful code than 201 (e.g. 200)
    if not resp.status_code == 201:
        msg = f' NOT OK: Responded with {resp.status_code} instead of 201.'
        print(msg)
        if RAISE_ERROR: raise ValueError(msg)
        return None
    print(' 1 -', end="", flush=True)

    # Polling (second request)
    url = resp.headers['location']
    job_id = None
    attempt = 0
    while True:
        attempt += 1
        resp = requests.get(url)
        # Get job_id once successful:
        if resp.json()['status'] == 'successful':
            job_id = resp.json()['jobID']
            print(' 2 - ', end="", flush=True)
            break
        print('-', end="", flush=True)
        # Stop after too many attempts:
        if attempt >= 100:
            msg = ' NOT OK: Stopping after 100 attempts.'
            print(msg)
            if RAISE_ERROR: raise ValueError(msg)
            return None
            break

    # Third request (for results)
    url = f'{pyserver}/jobs/{job_id}/results?f=json'
    resp = requests.get(url)
    print('3 -', end="", flush=True)
    return resp

def sanity_checks_geojson(resp, do_raise_error=True):
    ok = True

    if resp is None:
      return False

    # Check that it's a dict:
    resp_json = resp.json()
    if isinstance(resp_json, dict):
        pass
    else:
        ok = False
        msg = ' NOT OK: Not a dict.'
        print(msg)
        if do_raise_error: raise ValueError(msg)
        return False

    # Check that it has a 'type' key:
    if "type" in resp_json:
        pass
    else:
        ok = False
        msg =' NOT OK: Invalid GeoJSON: No "type".'
        print(msg)
        if do_raise_error: raise ValueError(msg)
        return False

    # Check that it can be parsed by geojson library:
    try:
        geojson_obj = geojson.loads(resp.text)  # can also use geojson.dumps(data)
    except (ValueError, TypeError) as e:
        ok = False
        msg = f' NOT OK: Invalid GeoJSON: {e}'
        print(msg)
        if do_raise_error: raise ValueError(msg)
        return False

    if ok:
        print(' OK.')
        return True


def sanity_checks_basic(resp, do_raise_error=True):
    ok = True

    if resp is None:
      return False

    # Check that it's a dict:
    resp_json = resp.json()
    if isinstance(resp_json, dict):
        pass
    else:
        ok = False
        msg = ' NOT OK: Not a dict.'
        print(msg)
        if do_raise_error: raise ValueError(msg)
        return False

    if ok:
        print(' OK.')
        return True


if __name__ == '__main__':

    print(f'Starting at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # For testing:
    #process_ids_one_pair = ["get-upstream-bbox"]
    #process_ids_two_pairs = ["get-shortest-path-between-points"]
    num = 0
    num_not_ok = 0

    try: # to catch KeyboardInterrupt...

        ############
        ### Sync ###
        ############

        if True:
            print('Synchronous...')

            ## One pair
            for process_id in process_ids_one_pair:
                print(f'Trying: {process_id}... ', end="", flush=True)  # no newline
                num += 1
                resp = make_sync_request(PYSERVER, process_id, PAYLOAD_ONE_PAIR)
                ok = sanity_checks_geojson(resp, RAISE_ERROR)
                if not ok: num_not_ok += 1

            ## Two pairs
            for process_id in process_ids_two_pairs:
                print(f'Trying: {process_id}... ', end="", flush=True)  # no newline
                num += 1
                resp = make_sync_request(PYSERVER, process_id, PAYLOAD_TWO_PAIRS)
                ok = sanity_checks_geojson(resp, RAISE_ERROR)
                if not ok: num_not_ok += 1

        #############
        ### Async ###
        #############

        if True:
            print('Asynchronous...')

            ## One pair
            for process_id in process_ids_one_pair:
                print(f'Trying: {process_id}... ', end="", flush=True)  # no newline
                num += 1
                resp = make_async_request(PYSERVER, process_id, PAYLOAD_ONE_PAIR)
                ok = sanity_checks_geojson(resp, RAISE_ERROR)
                if not ok: num_not_ok += 1

            ## Two pairs
            for process_id in process_ids_two_pairs:
                print(f'Trying: {process_id}... ', end="", flush=True)  # no newline
                num += 1
                resp = make_async_request(PYSERVER, process_id, PAYLOAD_TWO_PAIRS)
                ok = sanity_checks_geojson(resp, RAISE_ERROR)
                if not ok: num_not_ok += 1


        ###############
        ### Finally ###
        ###############
        print(f'Finished at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        if num_not_ok == 0:
            print(f'All ok: {num}/{num}.')
        else:
            print(f'Failed: {num_not_ok}/{num}.')


    except KeyboardInterrupt:
        print(f'\nInterrupted by user at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

        if num_not_ok == 0:
            print(f'Ok: {num}/{num}.')
        else:
            print(f'Failed: {num_not_ok}/{num}.')

