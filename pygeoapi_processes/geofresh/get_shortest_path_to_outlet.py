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
import pygeoapi.process.aqua90m.geofresh.routing as routing
import pygeoapi.process.aqua90m.geofresh.get_linestrings as get_linestrings
import pygeoapi.process.aqua90m.pygeoapi_processes.utils as utils
from pygeoapi.process.aqua90m.geofresh.database_connection import get_connection_object_config


'''
# Request a GeometryCollection (LineStrings):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-to-outlet/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.937520027160646,
    "lat": 54.69422745526058,
    "geometry_only": true,
    "comment": "bla"
    }
}'

# Request a FeatureCollection (LineStrings):
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-to-outlet/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.937520027160646,
    "lat": 54.69422745526058,
    "geometry_only": false,
    "add_downstream_ids": true,
    "comment": "bla"
    }
}'

# Request only the ids:
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-to-outlet/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.937520027160646,
    "lat": 54.69422745526058,
    "downstream_ids_only": true
    }
}'

# Request a FeatureCollection (LineStrings), but only up to strahler 3 (included)
# Tested: 2026-01-02
curl -X POST https://${PYSERVER}/processes/get-shortest-path-to-outlet/execution \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "lon": 9.937520027160646,
    "lat": 54.69422745526058,
    "geometry_only": false,
    "add_downstream_ids": true,
    "only_up_to_strahler": 4,
    "comment": "bla"
    }
}'

'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class ShortestPathToOutletGetter(BaseProcessor):

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
        return f'<ShortestPathToOutletGetter> {self.name}'


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
        lon_start = data.get('lon', None)
        lat_start = data.get('lat', None)
        subc_id1 = data.get('subc_id', None) # optional, need either lonlat OR subc_id
        comment = data.get('comment') # optional
        geometry_only = data.get('geometry_only', False)
        downstream_ids_only = data.get('downstream_ids_only', False)
        add_downstream_ids = data.get('add_downstream_ids', True)
        only_up_to_strahler = data.get('only_up_to_strahler', None)

        # Parse int:
        only_up_to_strahler = int(only_up_to_strahler) if only_up_to_strahler is not None else None

        # Check if boolean:
        utils.is_bool_parameters(dict(
            geometry_only=geometry_only,
            downstream_ids_only=downstream_ids_only,
            add_downstream_ids=add_downstream_ids,
        ))

        # Overall goal: Get the dijkstra shortest path (as linestrings)!

        # Get reg_id, basin_id, subc_id
        if subc_id1 is not None:
            # (special case: user provided subc_id instead of lonlat!)
            LOGGER.info(f'START: Getting dijkstra shortest path for subc_id {subc_id1} to sea')
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, subc_id = subc_id1)
        else:
            LOGGER.info(f'START: Getting dijkstra shortest path for lon {lon_start}, lat {lat_start} to sea')
            subc_id1, basin_id1, reg_id1 = basic_queries.get_subcid_basinid_regid(
                conn, LOGGER, lon_start, lat_start)

        # Outlet has minus basin_id as subc_id!
        subc_id2 = -basin_id1

        ##################
        ### Actual ... ###
        ##################

        # Potential result:
        json_result = {}

        # Get subc_ids of the whole connection...
        LOGGER.debug(f'Getting network connection for subc_id: start = {subc_id1}, end = {subc_id2}')
        segment_ids = routing.get_dijkstra_ids_one_to_one(conn, subc_id1, subc_id2, reg_id1, basin_id1)

        # Filter by strahler, e.g. only strahler orders 1-3, by specifying only_up_to_strahler = 3
        if only_up_to_strahler is not None:
            LOGGER.debug(f'User requested to exclude {only_up_to_strahler} and higher...')
            LOGGER.debug(f'Before filtering by strahler: {len(segment_ids)}')
            segment_ids = filter_subcid_by_strahler(
                conn, segment_ids, reg_id1, basin_id1, only_up_to_strahler)
            LOGGER.debug(f'After filtering by strahler: {len(segment_ids)}')

        # Only return the ids, no geometry at all:
        if downstream_ids_only:
            json_result["downstream_ids"] = segment_ids

        # Get GeometryCollection only:
        elif geometry_only:
            json_result = get_linestrings.get_streamsegment_linestrings_geometry_coll(
                conn, segment_ids, basin_id1, reg_id1)

        # Get FeatureCollection
        if not geometry_only and not downstream_ids_only:
            json_result = get_linestrings.get_streamsegment_linestrings_feature_coll(
                conn, segment_ids, basin_id1, reg_id1)

            # Add some info to the FeatureCollection:
            # TODO: Should we include the requested lon and lat? Maybe as a point?
            json_result["description"] = f"Downstream path from subcatchment {subc_id1} to the outlet of its basin."
            json_result["subc_id"] = subc_id1 # TODO how to name the point from where we route to outlet?
            json_result["outlet_id"] = subc_id2
            json_result["downstream_path_of"] = subc_id1
            if add_downstream_ids:
                json_result["downstream_ids"] = segment_ids


        ##############
        ### Return ###
        ##############
            
        if comment is not None:
            json_result['comment'] = comment

        # Return link to result (wrapped in JSON) if requested, or directly the JSON object:
        if utils.return_hyperlink('downstream_path', requested_outputs):
            output_dict_with_url =  utils.store_to_json_file('downstream_path', json_result,
                self.metadata, self.job_id,
                self.download_dir,
                self.download_url)
            return 'application/json', output_dict_with_url
        else:
            return 'application/json', json_result


# Util for filtering
def filter_subcid_by_strahler(conn, subc_ids, reg_id, basin_id, only_up_to_strahler):
    # INPUT:  List of subc_ids (integers)
    # OUTPUT: List of subc_ids (integers)

    ## Check if strahler is integer:
    only_up_to_strahler = int(only_up_to_strahler)

    ## Get strahler attribute from database:
    columns=['subc_id', 'basin_id', 'reg_id', 'strahler']
    temp_df = basic_queries.get_basinid_regid_from_subcid_plural(
        conn, LOGGER, subc_ids, columns=columns)
    LOGGER.debug(f'BEFORE: {temp_df}')

    ## Filter dataframe:
    output_df = temp_df[temp_df["strahler"] <= only_up_to_strahler]
    LOGGER.debug(f'AFTER: {output_df}')

    ## Return list:
    filtered_subc_ids = output_df["subc_id"].astype(int).tolist()
    return filtered_subc_ids

