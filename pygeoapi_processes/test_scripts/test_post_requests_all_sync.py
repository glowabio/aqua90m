import requests
import sys
import datetime

'''
This is just a little script to test whether the OGC processing
services of GeoFREHS / hydrographr were properly
installed using pygeoapi and run as expected.
This does not test any edge cases, just a very basic setup. The input
data may already be on the server, so proper downloading is not 
guaranteed.

Check the repository here:
https://github.com/glowabio/geofresh
https://glowabio.github.io/hydrographr/

Merret Buurman (IGB Berlin), 2024-08-20
'''


base_url = 'https://xxx.xxx/pygeoapi'
headers  = {'Content-Type': 'application/json'}
#headers = {'Content-Type': 'application/json', 'Prefer': 'respond-async'}


# Get started...
session = requests.Session()


'''
Local catchment:
  1  get_local_subcids.py
  1b get_local_subcids_plural.py
  2  get_snapped_points.py
  2b get_snapped_points_plus.py
  2c get_snapped_points_plural.py
  3  get_local_streamsegments.py
  3b get_local_streamsegments_subcatchments.py

Routing:
  4  get_shortest_path_to_outlet.py
  5  get_shortest_path_two_points.py
  5b get_shortest_path_between_points_plural.py

Upstream catchment:
  6  get_upstream_subcids.py
  7  get_upstream_streamsegments.py
  8  get_upstream_subcatchments.py
  9  get_upstream_bbox.py
  10 get_upstream_dissolved.py
  11 get_upstream_dissolved_aip.py

'''


#####################
#####################
### Local queries ###
#####################
#####################

###########################
### 1 get-local-subcids ###
###########################
name = "get-local-subcids"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "schlei-near-rabenholz"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###################################
### 1b get-local-subcids-plural ###
###################################
name = "get-local-subcids-plural"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
points_input = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {},
            "geometry": { "coordinates": [ 10.698832912677716, 53.51710727672125 ], "type": "Point" }
        },
        {
            "type": "Feature",
            "properties": {},
            "geometry": { "coordinates": [ 12.80898022975407, 52.42187129944509 ], "type": "Point" }
        },
        {
            "type": "Feature",
            "properties": {},
            "geometry": { "coordinates": [ 11.915323076217902, 52.730867141970464 ], "type": "Point" }
        },
        {
            "type": "Feature",
            "properties": {},
            "geometry": { "coordinates": [ 16.651903948708565, 48.27779486850176 ], "type": "Point" }
        },
        {
            "type": "Feature",
            "properties": {},
            "geometry": { "coordinates": [ 19.201146608148463, 47.12192880511424 ], "type": "Point" }
        },
        {
            "type": "Feature",
            "properties": {},
            "geometry": { "coordinates": [ 24.432498016999062, 61.215505889934434 ], "type": "Point" }
        }
    ]
}
inputs = {
    "inputs": {
        "points_geojson": points_input,
        "comment": "schlei-near-rabenholz"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


############################
### 2 get-snapped-points ###
############################
name = "get-snapped-points"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "schlei-near-rabenholz",
        "geometry_only": "false"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


####################################
### 2 get-snapped-point          ###
### Special case: Outside Europe ###
####################################
## Outside europe, so we expect an error!
name = "get-snapped-points"
print('\n##### Calling %s... #####' % name)
print('Calling the same process, but coordinates outside Europe! We expect it to fail.')
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "72.5",
        "lat": "83.5",
        "comment": "outside europe"
    }
}
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 400: # expecting error
    print('Error, as expected:')
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


#################################
### 2b get-snapped-point-plus ###
#################################
name = "get-snapped-point-plus"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "schlei-near-rabenholz"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)

####################################
### 2c get-snapped-points-plural ###
####################################
name = "get-snapped-points-plural"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "points": {
          "type": "MultiPoint",
          "coordinates": [
            [9.937520027160646, 54.69422745526058],
            [9.9217, 54.6917],
            [9.9312, 54.6933]
          ]
        },
        "comment": "schlei-near-rabenholz"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


##################################
### 3 get-local-streamsegments ###
##################################
name = "get-local-streamsegments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "schlei-near-rabenholz",
        "geometry_only": "false"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


#################################################
### 3b get-local-streamsegments-subcatchments ###
#################################################
name = "get-local-streamsegments-subcatchments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "schlei-near-rabenholz",
        "geometry_only": "false"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



#######################
#######################
### Routing queries ###
#######################
#######################


#####################################
### 4 get-shortest-path-to-outlet ###
#####################################
name = 'get-shortest-path-to-outlet'
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "geometry_only": "false",
        "add_downstream_ids": "true",
        "comment": "located in schlei area"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


##########################################
### 5 get-shortest-path-between-points ###
##########################################
name = 'get-shortest-path-between-points'
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon_start": "9.937520027160646",
        "lat_start": "54.69422745526058",
        "lon_end": "9.9217",
        "lat_end": "54.6917",
        "comment": "test bla",
        "geometry_only": "false",
        "add_segment_ids": "true"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not "subc_ids" in resp.json():
        print('Missing: segment_ids')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)

##################################################
### 5b get-shortest-path-between-points-plural ###
##################################################
name = 'get-shortest-path-between-points-plural'
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "points": {
          "type": "MultiPoint",
          "coordinates": [
            [9.937520027160646, 54.69422745526058],
            [9.9217, 54.6917],
            [9.9312, 54.6933]
          ]
        },
        "comment": "test bla",
        "geometry_only": "false",
        "add_segment_ids": "true"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    print('TODO: Method is still WIP, need to check results once it is finished...')
    #if not "segment_ids" in resp.json():
    #    print('Missing: segment_ids')
    #    sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


########################
########################
### Upstream queries ###
########################
########################


##############################
### 6 get-upstream-subcids ###
##############################
name = "get-upstream-subcids"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "comment": "schlei-near-rabenholz"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


##########################################
### 6 get-upstream-subcids             ###
### Special case: Test passing subc_id ###
##########################################
name = "get-upstream-subcids"
print('\n##### Calling %s... #####' % name)
print('Input: subc_id this time, not lon lat!')
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "subc_id": "506245899",
        "comment": "located in nordfriesland"
        #"subc_id": "553495421",
        #"comment": "located in vantaanjoki area, finland"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


##################################
### 6 get-upstream-subcids     ###
### Special case: Exceed max   ###
### num of upstream catchments ###
##################################
name = "get-upstream-subcids"
print('\n##### Calling %s... #####' % name)
print('This one should not pass, because we restrict to 200 upstream catchments...')
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.109039306640627",
        "lat": "52.7810591224723",
        "comment": "this has 403 upstream catchments"
    }
}
inputs = {
    "inputs": {
        "lon": "9.17770385",
        "lat": "52.957628575",
        "comment": "this has 208433 upstream catchments"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 400: # expecting error
    print('Response content: %s' % resp.json())
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


#####################################
### 7 get-upstream-streamsegments ###
#####################################
name = "get-upstream-streamsegments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "geometry_only": "false",
        "add_upstream_ids": "true",
        "comment": "schlei-near-rabenholz"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not 'subc_ids' in resp.json():
        print('Missing: subc_ids')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


####################################
### 8 get-upstream-subcatchments ###
####################################
name = "get-upstream-subcatchments"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "geometry_only": "false",
        "add_upstream_ids": "true",
        "comment": "schlei-near-rabenholz"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not 'subc_ids' in resp.json():
        print('Missing: subc_ids')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###########################
### 9 get-upstream-bbox ###
###########################
name = "get-upstream-bbox"
print('\n##### Calling %s... #####' % name)
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "geometry_only": "false",
        "add_upstream_ids": "true",
        "comment": "schlei-near-rabenholz"
    }
}
print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not 'subc_ids' in resp.json()['properties']:
        print('Missing: subc_ids')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


######################################
### 10 get-upstream-dissolved-cont ###
######################################
name = "get-upstream-dissolved-cont"
print('\n##### Calling %s... #####' % name)
print('Asking for default, which is "value"')
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": "9.931555",
        "lat": "54.695070",
        "geometry_only": "false",
        "add_upstream_ids": "true",
        "comment": "schlei-near-rabenholz"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not 'subc_ids' in resp.json()['properties']:
        print('Missing: subc_ids')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)



######################################
### 10b get-upstream-dissolved-aip ###
######################################
name = "get-upstream-dissolved"
print('\n##### Calling %s... #####' % name)
print('Asking for default, which is "value"')
url = base_url+'/processes/%s/execution' % name
inputs = {
    "inputs": {
        "lon": 9.931555,
        "lat": 54.695070,
        "get_type": "polygon",
        "comment": "schlei-near-rabenholz"
    }
}

print('\nSynchronous %s...' % name)
resp = session.post(url, headers=headers, json=inputs)
print('### Calling %s... done. HTTP %s' % (name, resp.status_code))
if resp.status_code == 200:
    print('Response content: %s' % resp.json())
    if not 'polygon' in resp.json()['outputs']:
        print('Missing: polygon')
        sys.exit(1)
    if not 'href' in resp.json()['outputs']['polygon']:
        print('Missing: href')
        sys.exit(1)
else:
    print('%s> HTTP %s <%s' % (70*'-', resp.status_code, 100*'-'))
    print('Response content: %s' % resp.json())
    print('Failed. Stopping...')
    sys.exit(1)


###################
### Finally ... ###
###################
print('\nDone!')

