import logging
LOGGER = logging.getLogger(__name__)

import os
import sys
import traceback
import json
import psycopg2
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import pygeoapi.process.aqua90m.geofresh.basic_queries as basic_queries
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


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
        "comment": "schlei-near-rabenholz"
    }
}'


# Request plain JSON (not GeoJSON: Cannot request Feature/Geometry, does not apply)
# Input points: Lonlat string
curl -X POST --location 'http://localhost:5000/processes/get-local-subcids-plural/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "lonlatstring": "10.698832912677716,53.51710727672125;12.80898022975407,52.42187129944509;11.915323076217902,52.730867141970464;16.651903948708565,48.27779486850176;19.201146608148463,47.12192880511424;24.432498016999062,61.215505889934434",
        "comment": "schlei-near-rabenholz"
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


class LocalSubcidGetterPlural(BaseProcessor):

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
        return f'<LocalSubcidGetterPlural> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the subcatchment from coordinates..."')
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

        # User inputs: GeoJSON...
        points_geojson = data.get('points_geojson', None)
        # ... or values in a comma separated string, or in CSV: # Still WIP TODO
        csv = data.get('csv', None)
        lonlatstring = data.get('lonlatstring', None)
        #colname_lat = data.get('colname_lat', 'lat')
        #colname_lon = data.get('colname_lat', 'lon')
        #subc_id = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional

        ########################################################
        ### Package all points as GeoJSON GeometryCollection ###
        ########################################################

        all_points = geojson_helpers.any_points_to_MultiPointGeometryCollection(LOGGER,
            points_geojson = points_geojson,
            lonlatstring = lonlatstring,
            csv = csv)

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

        output_json = basic_queries.get_subc_id_basin_id_reg_id_for_all(
            conn, LOGGER, all_points)

        ################
        ### Results: ###
        ################

        '''
        TODO: Next step, we need bbox per reg_id / per basin_id / per subc_id...
        Then, subset using that bbox.
        Or better, get polygon per reg_id / basin_id / subc_id ? GeoFRESH contains these, but then use them directly for subsetting?
        '''


        if output_json is not None:
            output_json['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        # In this case, storing a JSON file is totally overdone! But for consistency's sake...
        if utils.return_hyperlink('subc_id', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('subc_id', output_json,
                self.metadata, self.job_id,
                self.config['download_dir'],
                self.config['download_url'])
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', output_json

