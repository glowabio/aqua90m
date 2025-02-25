import logging
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import psycopg2

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

try:
    import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
    from pygeoapi.process.aqua90m.geofresh.py_query_db import get_connection_object
except ModuleNotFoundError:
    #from aqua90m.geofresh.basic_queries import get_subc_id_basin_id_reg_id
    #from aqua90m.geofresh.py_query_db import get_connection_object
    import geofresh.basic_queries as basic_queries
    from geofresh.py_query_db import get_connection_object


'''
# Request plain JSON (not GeoJSON: Cannot request Feature/Geometry, does not apply)
# Input points: GeoJSON
curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "points_geojson": {
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
        },
        "comment": "schlei-bei-rabenholz"
    }
}'


# Request plain JSON (not GeoJSON: Cannot request Feature/Geometry, does not apply)
# Input points: Lonlat string
curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "lonlatstring": "10.698832912677716,53.51710727672125;12.80898022975407,52.42187129944509;11.915323076217902,52.730867141970464;16.651903948708565,48.27779486850176;19.201146608148463,47.12192880511424;24.432498016999062,61.215505889934434",
        "comment": "schlei-bei-rabenholz"
    }
}'

'''

'''
Example result:
# TODO: Discuss if this result is understandable enough!
{
    "subc_ids":"507307015, 90929627, 553374842, 507081236, 506601172, 91875954",
    "region_ids":"58, 65, 159",
    "basin_ids":"1291835, 1291763, 1294020",
    "everything":{
        "58":{
            "1294020":{
                "506601172":[
                    {
                        "coordinates":[
                            10.698832912677716,
                            53.51710727672125
                        ],
                        "type":"Point"
                    }
                ],
                "507307015":[
                    {
                        "coordinates":[
                            12.80898022975407,
                            52.42187129944509
                        ],
                        "type":"Point"
                    }
                ],
                "507081236":[
                    {
                        "coordinates":[
                            11.915323076217902,
                            52.730867141970464
                        ],
                        "type":"Point"
                    }
                ]
            }
        },
        "159":{
            "1291835":{
                "90929627":[
                    {
                        "coordinates":[
                            16.651903948708565,
                            48.27779486850176
                        ],
                        "type":"Point"
                    }
                ],
                "91875954":[
                    {
                        "coordinates":[
                            19.201146608148463,
                            47.12192880511424
                        ],
                        "type":"Point"
                    }
                ]
            }
        },
        "65":{
            "1291763":{
                "553374842":[
                    {
                        "coordinates":[
                            24.432498016999062,
                            61.215505889934434
                        ],
                        "type":"Point"
                    }
                ]
            }
        }
    },
    "comment": "schlei-bei-rabenholz"
}

TODO: Maybe rather:


{
    "subc_ids":"507307015, 90929627, 553374842, 507081236, 506601172, 91875954",
    "region_ids":"58, 65, 159",
    "basin_ids":"1291835, 1291763, 1294020",
    "everything":{
        "regional_unit_ids": {
            "58": {
                "basin_ids": {
                    "1294020": {
                        "subcatchment_ids": {
                            "506601172": [...point, point, point...],
                            "50660117x": [...point, point, point...],
                        }
                    }
                }
            }
        }
    }
        

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class LocalSubcidPluralGetter(BaseProcessor):

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
        return f'<LocalSubcidGetter> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the subcatchment from coordinates..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        try:
            conn = get_db_connection(self.config)
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
        csv = data.get('csv', None)
        lonlatstring = data.get('lonlatstring', None)
        colname_lat = data.get('colname_lat', 'lat')
        colname_lon = data.get('colname_lat', 'lon')
        points_geojson = data.get('points_geojson', None)
        #subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional

        ########################################################
        ### Package all points as GeoJSON GeometryCollection ###
        ########################################################

        # We collect all points as GeoJSON:
        all_points = {
            'type': 'GeometryCollection',
            'geometries': []
        }

        # Quick and dirty: Points are provided as pairs in string:
        if lonlatstring is not None:
            LOGGER.debug('Found lon,lat pairs: %s' % lonlatstring)

            lonlat = lonlatstring.split(';')
            num_pairs = 0
            for coordinate_pair in lonlat:
                lon, lat = coordinate_pair.split(',')
                all_points['geometries'].append({'type':'Point', 'coordinates':[lon, lat]})
                LOGGER.debug('Added: %s' % {'type':'Point', 'coordinates':[lon, lat]})
                num_pairs += 1

            LOGGER.info('Found %s lon,lat pairs...' % num_pairs)

        # Points are provided in CSV columns:
        elif csv is not None:
            pass
            LOGGER.error('Not implemented!!')
            raise ProcessorExecuteError('Not implemented!!')

            '''
            # In which format do we get it? Just as string I guess...
            # Add the received input to temp dir:
            output_temp_dir = WIP TODO
            input_temp_path = output_temp_dir+os.sep+'someInputsWIP.csv'
            with open(input_temp_path, 'w') as inputfile:
                inputfile.write(csv)
                # TODO Clean up csv files!
            '''
            

        elif points_geojson is not None:

            if points_geojson['type'] == 'GeometryCollection':
                
                for point in points_geojson['geometries']:
                    if not point['type'] == 'Point':
                        err_msg = 'Geometries in GeometryCollection have to be points, not: %s' % feature['type']
                        LOGGER.error(err_msg)
                        raise ProcessorExecuteError(err_msg)
                
                all_points = points_geojson

            elif points_geojson['type'] == 'FeatureCollection':

                for feature in points_geojson['features']:
                    if not feature['geometry']['type'] == 'Point':
                        err_msg = 'Features in FeatureCollection have to be points, not: %s' % feature['type']
                        LOGGER.error(err_msg)
                        raise ProcessorExecuteError(err_msg)

                    all_points['geometries'].append(feature['geometry'])



        '''
        * Check bbox --> Europe, America, ...? Possible regional units!
        * Run spatial intersection with basins, for each possible regional unit, one after the other, until match?
        * Now we have the basin!
        * Return basin... All requested data for the entire basin? Prepare shapes?
        * Do we also return all subc_ids? All requested data for only those subc_ids?
        '''

        ###############################
        ### Get info point by point ###
        ###############################

        reg_ids, basin_ids, subc_ids, everything = basic_queries.get_subc_id_basin_id_reg_id_for_all(
            conn, LOGGER, all_points)




        ################
        ### Results: ###
        ################

        '''
        TODO: Next step, we need bbox per reg_id / per basin_id / per subc_id...
        Then, subset using that bbox.
        Or better, get polygon per reg_id / basin_id / subc_id ? GeoFRESH contains these, but then use them directly for subsetting?
        '''

        # Note: This is not GeoJSON (on purpose), as we did not look for geometry yet.
        output = {
            "subc_ids":   ', '.join(str(i) for i in set(subc_ids)),
            "region_ids": ', '.join(str(i) for i in set(reg_ids)),
            "basin_ids":  ', '.join(str(i) for i in set(basin_ids)),
            "everything": everything
        }

        if comment is not None:
            output['comment'] = comment

        # In this case, storing a JSON file is totally overdone! But for consistency's sake...
        if self.return_hyperlink('subc_id', requested_outputs):
            return 'application/json', self.store_to_json_file('subc_id', output)
        else:
            return 'application/json', output


    def return_hyperlink(self, output_name, requested_outputs):

        if requested_outputs is None:
            return False

        if 'transmissionMode' in requested_outputs.keys():
            if requested_outputs['transmissionMode'] == 'reference':
                return True

        if output_name in requested_outputs.keys():
            if 'transmissionMode' in requested_outputs[output_name]:
                if requested_outputs[output_name]['transmissionMode'] == 'reference':
                    return True

        return False


    def store_to_json_file(self, output_name, json_object):

        # Store to file
        downloadfilename = 'outputs-%s-%s.json' % (self.metadata['id'], self.job_id)
        downloadfilepath = self.config['download_dir']+downloadfilename
        LOGGER.debug('Writing process result to file: %s' % downloadfilepath)
        with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
            json.dump(json_object, downloadfile, ensure_ascii=False, indent=4)

        # Create download link:
        downloadlink = self.config['download_url'] + downloadfilename

        # Create output to pass back to user
        outputs_dict = {
            'title': self.metadata['outputs'][output_name]['title'],
            'description': self.metadata['outputs'][output_name]['description'],
            'href': downloadlink
        }

        return outputs_dict


def get_db_connection(config):

    geofresh_server = config['geofresh_server']
    geofresh_port = config['geofresh_port']
    database_name = config['database_name']
    database_username = config['database_username']
    database_password = config['database_password']
    use_tunnel = config.get('use_tunnel')
    ssh_username = config.get('ssh_username')
    ssh_password = config.get('ssh_password')
    localhost = config.get('localhost')

    try:
        conn = get_connection_object(geofresh_server, geofresh_port,
            database_name, database_username, database_password,
            use_tunnel=use_tunnel, ssh_username=ssh_username, ssh_password=ssh_password)
    except sshtunnel.BaseSSHTunnelForwarderError as e1:
        LOGGER.error('SSH Tunnel Error: %s' % str(e1))
        raise e1

    return conn


def get_subc_id_basin_id_reg_id_for_all(conn, LOGGER, points_geojson):
    # TODO: This is not super efficient, but the quickest to implement :)
    dummy = None
    everything = {}
    basin_ids = []
    reg_ids = []
    subc_ids = []
    num = len(points_geojson['geometries'])
    for point in points_geojson['geometries']:
        lon, lat = point['coordinates']
        print('Getting subcatchment for lon, lat: %s, %s' % (lon, lat))
        subc_id, basin_id, reg_id = get_subc_id_basin_id_reg_id(conn, LOGGER, lon, lat, dummy)
        print('TYPES: %s %s %s' % (type(reg_id), type(basin_id), type(subc_id)))
        print('VALUES: %s %s %s' % (reg_id, basin_id, subc_id))
        reg_ids.append(str(reg_id))
        basin_ids.append(str(basin_id))
        subc_ids.append(str(subc_id))

        if not reg_id in everything:
            everything[reg_id] = {basin_id: {subc_id: [point]}}
        else:
            if not basin_id in everything[reg_id]:
                everything[reg_id][basin_id] = {subc_id: [point]}
            else:
                if not subc_id in everything[reg_id][basin_id]:
                    everything[reg_id][basin_id][subc_id] = [point]
                else:
                    everything[reg_id][basin_id][subc_id].append(point)


    # Extensive logging...
    LOGGER.info('Of %s points, ...')
    
    # Stats reg_id...
    if len(set(reg_ids)) == 1:
        LOGGER.info('... all %s points fall into regional unit with reg_id %s' % (num, reg_ids[0]))
    else:
        reg_id_counts = {reg_id: reg_ids.count(reg_id) for reg_id in reg_ids}
        for reg_id in set(reg_ids):
            LOGGER.info('... %s points fall into regional unit with reg_id %s' % (reg_id_counts[reg_id], reg_id))
    
    # Stats basin_id...
    if len(set(basin_ids)) == 1:
        LOGGER.info('... all %s points fall into drainage basin with basin_id %s' % (num, basin_ids[0]))
    else:
        #basin_id_counts = {basin_id:basin_ids.count(i) for basin_id in basin_ids}
        basin_id_counts = {basin_id: basin_ids.count(basin_id) for basin_id in basin_ids}
        for basin_id in set(basin_ids):
            LOGGER.info('... %s points fall into drainage basin with basin_id %s' % (basin_id_counts[basin_id], basin_id))

    # Stats subc_id...
    if len(set(subc_ids)) == 1:
        LOGGER.info('... all %s points fall into subcatchment with subc_id %s' % (num, subc_ids[0]))
    else:
        #subc_id_counts = {subc_id:subc_ids.count(i) for subc_id in subc_ids}
        subc_id_counts = {subc_id: subc_ids.count(subc_id) for subc_id in subc_ids}
        for subc_id in set(subc_ids):
            LOGGER.info('... %s points fall into subcatchment with subc_id %s' % (subc_id_counts[subc_id], subc_id))

    return reg_ids, basin_ids, subc_ids, everything


if __name__ == '__main__':
    # TODO Move this to a different module!!!
    print("In module products __package__, __name__ ==", __package__, __name__)

    points_geojson = {
        'type': 'GeometryCollection',
        'geometries': [TODO]
    }


    config = json.loads('/home/.../config.json')

    conn = get_db_connection(config)

    reg_ids, basin_ids, subc_ids, everything = get_subc_id_basin_id_reg_id_for_all(conn, LOGGER, points_geojson)

    # Note: This is not GeoJSON (on purpose), as we did not look for geometry yet.
    output = {
        "subc_ids":   ', '.join(str(i) for i in set(subc_ids)),
        "region_ids": ', '.join(str(i) for i in set(reg_ids)),
        "basin_ids":  ', '.join(str(i) for i in set(basin_ids)),
        "everything": everything
    }

    print('HERE WE GO:\n%s' % output)
    print('DONNEEEE')

