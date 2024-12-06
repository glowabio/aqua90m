import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import argparse
import os
import sys
import traceback
import json
import subprocess
import pandas as pd

'''
# Input points: lonlatstring
# Input raster: variable name, local tif from lookuptable
# Outputs: json and csv, both as reference
curl -X POST "http://localhost:5000/processes/extract-point-stats/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "variable_name": "sti",
    "lonlatstring": "lon lat\n5.5 52.7\n7.3 51.6",
    "colname_lon": "lon",
    "colname_lat": "lat",
    "comment": "test 1"
  },
  "outputs": {
    "transmissionMode": "reference"
  }
}'

# Input points: lonlatstring
# Input raster: variable name, local tif from lookuptable
# Outputs: Requested nothing, so getting all back as reference...
curl -X POST "http://localhost:5000/processes/extract-point-stats/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "variable_name": "sti",
    "lonlatstring": "lon lat\n5.5 52.7\n7.3 51.6",
    "colname_lon": "lon",
    "colname_lat": "lat",
    "comment": "test 2"
  }
}'


# Input points: lonlatstring
# Input raster: variable name, Allas VRT from lookuptable
# Outputs: json and csv, both as value
curl -X POST "http://localhost:5000/processes/extract-point-stats/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "variable_name": "basin",
    "lonlatstring": "lon lat\n5.5 52.7\n7.3 51.6",
    "colname_lon": "lon",
    "colname_lat": "lat",
    "comment": "test 3"
  },
  "outputs": {
    "transmissionMode": "value"
  }
}'


# Input points: GeoJSON
# Input raster: variable name, Allas VRT from lookuptable
# Outputs: json and csv, both as value
curl -X POST "http://localhost:5000/processes/extract-point-stats/execution" \
--header "Content-Type: application/json" \
--data '{
  "inputs": {
    "variable_name": "basin",
    "points_geojson": {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-16.1547,66.3945]},
                "properties": {"comment": "North East Iceland, tile h16v00"}
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [24.7615,56.8253]},
                "properties": {"comment": "South East of Riga, tile h20v02"}
            }
        ]
    },
    "comment": "test 4"
  },
  "outputs": {
    "geojson": "value",
    "csv": "reference"
  }
}'


'''

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class ExtractPointStatsProcessor(BaseProcessor):

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
        return f'<ExtractPointStatsProcessor> {self.name}'


    def execute(self, data, outputs=None):
        LOGGER.info('Starting to get the point stats from coordintes..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        if outputs is None:
            outputs = {'ALL': 'dummy'}
        elif set(outputs.keys()) == set(['transmissionMode']):
            outputs['ALL'] = 'dummy'

        try:
            res = self._execute(data, outputs)
            return res

        except Exception as e:
            LOGGER.error('During process execution, this happened: %s' % e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?


    def _execute(self, data, requested_outputs):

        # User inputs
        lonlatstring = data.get('lonlatstring', None)
        variable_name = data.get('variable_name', None)  # names, e.g. "spi", "sti" --> enum!
        comment = data.get('comment') # optional
        colname_lat = data.get('colname_lat', None)
        colname_lon = data.get('colname_lon', None)
        points_geojson = data.get('points_geojson', None)
        points_geojson_url = data.get('points_geojson_url')
        variable_layer_url = data.get('variable_layer_url', None)

        # Check user inputs
        if variable_name is None and variable_layer_url is None:
            raise ProcessorExecuteError('Need to provide "variable_name" or "variable_layer_url".')
        if lonlatstring is None and points_geojson is None and points_geojson_url is None:
            raise ProcessorExecuteError('Need to provide "lonlatstring" or "points_geojson" or "points_geojson_url".')
        elif sum([lonlatstring is not None,
                  points_geojson is not None,
                  points_geojson_url is not None]) > 1:
            raise ProcessorExecuteError('Need to provide either "lonlatstring" or "points_geojson" or "points_geojson_url", just one of them.')
        elif lonlatstring is not None:
            if colname_lat is None or colname_lon is None:
                 raise ProcessorExecuteError('If you provide "lonlatstring", you also must provide "colname_lat" and "colname_lon".')
            if not colname_lat in lonlatstring:
                 raise ProcessorExecuteError('If you provide "lonlatstring", you also must provide matching "colname_lat".')
            if not colname_lon in lonlatstring:
                 raise ProcessorExecuteError('If you provide "lonlatstring", you also must provide matching "colname_lon".')

        ####################
        ### Points layer ###
        ####################

        # Write user-provided points to tmp
        # (as input for gdallocation info):
        coord_tmp_path = '/tmp/inputcoordinates_%s.txt' % self.job_id

        # User provided URL to GeoJSON points:
        if points_geojson_url is not None:
            resp = requests.get(points_geojson_url)
            if resp.status_code == 200:
                points_geojson = resp.json()

        # User provided points as feature collection:
        if points_geojson is not None:
            LOGGER.debug('Client provided a GeoJSON FeatureCollection...')
            # TODO Should we validate it?
            with open(coord_tmp_path, 'w') as myfile:
                colname_lon = 'lon'
                colname_lat = 'lat'
                myfile.write('lon lat\n')
                for item in points_geojson['features']:
                    coord_pair = item['geometry']['coordinates']
                    myfile.write('%s %s\n' % (coord_pair[0], coord_pair[1]))

        # User provided points as space-separated string:
        elif lonlatstring is not None:
            LOGGER.debug('Client provided coordinates in a string...')
            with open(coord_tmp_path, 'w') as myfile:
                myfile.write(lonlatstring)

        LOGGER.debug('Written user input lon lat to file: %s' % coord_tmp_path)

        ####################
        ### Raster layer ###
        ####################

        if variable_layer_url is not None:
            var_layer = variable_layer_url

        else:
            LOGGER.debug('Requested variable "%s"...' % variable_name)

            # Read path from config:
            path_tiffs = self.config['path_hy90m_rasters'].rstrip('/')

            # EITHER: Path and filename are always the same, given the variable name:
            # TODO: Test case, individual tiles, no VRT yet, so file id has to be specified!
            var_layer = '{path}/{var}_{tile}.tif'.format(
                path = path_tiffs, var=variable_name, tile='18v02')

            # OR: Path and filename are read from this Lookup Table:
            # TODO: Have an entire Lookup Table stored somewhere? Maybe in config?
            lookup_vrt = {
                "basin": "https://2007367-nextcloud.a3s.fi/igb/vrt/basin.vrt",
                "sti": path_tiffs+'/sti_h18v02.tif',
                "cti": "not-yet"
            }
            var_layer = lookup_vrt[variable_name]

        LOGGER.debug('Requested variable file "%s"...' % var_layer)

        # Where to store results:
        out_dir = self.config['download_dir'].rstrip('/')
        out_path_txt = out_dir+'/outputs_%s_%s_%s.txt' % (self.metadata['id'], variable_name, self.job_id)
        out_path_csv = out_dir+'/outputs_%s_%s_%s.csv' % (self.metadata['id'], variable_name, self.job_id)
        LOGGER.debug('Will write final result here: %s' % out_path_txt)

        # Run bash script
        path_bash_scripts = self.config['hydrographr_bash_files']
        args = [coord_tmp_path, colname_lon, colname_lat, var_layer, variable_name, out_dir, out_path_txt]
        returncode, stdouttext, stderrtext, err_msg = call_bash_script(LOGGER, "extract_point_stats.sh", path_bash_scripts, args)
        if not returncode == 0:
            raise ProcessorExecuteError(user_msg = err_msg)

        ################
        ### Results: ###
        ################

        outputs = {}

        output_name = "geojson"
        if output_name in requested_outputs.keys() or "ALL" in requested_outputs.keys():

            # TODO: Add properties of original feature collection?

            # Make GeoJSON from it:
            df = pd.read_csv(out_path_txt, sep=" ")
            def create_geojson_feature(row):
                return {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [row['lon'], row['lat']]
                    },
                    "properties": {
                        variable_name: row[variable_name]
                    }
                }

            geojson_features = df.apply(create_geojson_feature, axis=1).tolist()
            geojson = {
                "type": "FeatureCollection",
                "features": geojson_features
            }

            if self.return_hyperlink(output_name, requested_outputs):

                downloadlink = self.store_to_json_file(output_name, geojson)
                outputs[output_name] = {
                    "title": self.metadata['outputs'][output_name]['title'],
                    "description": self.metadata['outputs'][output_name]['description'],
                    "value": downloadlink
                }
            else:
                outputs[output_name] = {
                    "title": self.metadata['outputs'][output_name]['title'],
                    "description": self.metadata['outputs'][output_name]['description'],
                    "value": geojson
                }

        output_name = "csv"
        if output_name in requested_outputs.keys() or "ALL" in requested_outputs.keys():

            if self.return_hyperlink(output_name, requested_outputs):

                # Transform from space-separated to semicolon-separated:
                with open(out_path_txt, 'r') as txtfile:
                    with open(out_path_csv, 'w') as csvfile:
                        line = txtfile.read().replace(' ', ';')
                        csvfile.write(line)

                # Make and return download link:
                downloadlink = out_path_csv.replace(
                    self.config['download_dir'],
                    self.config['download_url']
                )

                outputs[output_name] = {
                    "title": self.metadata['outputs'][output_name]['title'],
                    "description": self.metadata['outputs'][output_name]['description'],
                    "value": downloadlink
                }

            else:

                resultstring = ''
                # Transform from space-separated to semicolon-separated:
                with open(out_path_txt, 'r') as txtfile:
                    with open(out_path_csv, 'w') as csvfile:
                        line = txtfile.read().replace(' ', ';')
                        resultstring += line+'\n'

                outputs[output_name] = {
                    "title": self.metadata['outputs'][output_name]['title'],
                    "description": self.metadata['outputs'][output_name]['description'],
                    "value": resultstring.rstrip("\n")
                }

        if comment is not None:
            outputs['comment'] = comment

        return 'application/json', outputs


    def return_hyperlink(self, output_name, requested_outputs):

        # No requested outputs: Returning reference per default (against specs!)
        if requested_outputs is None:
            LOGGER.debug('Client asked for no specific return type: Returning reference (against specs).')
            return True

        # Requested specific transmissionMode for all outputs:
        if 'transmissionMode' in requested_outputs.keys():
            if requested_outputs['transmissionMode'] == 'reference':
                LOGGER.debug('Client asked for reference for all outputs...')
                return True
            elif requested_outputs['transmissionMode'] == 'value':
                LOGGER.debug('Client asked for value for all outputs...')
                return False

        # Specific requests per individual output:
        if output_name in requested_outputs.keys():

            # Client requests reference/value directly (probably not compliant to specs):
            if requested_outputs[output_name] == 'reference':
                LOGGER.debug('Client asked for reference for output "%s"' % output_name)
                return True
            elif requested_outputs[output_name] == 'value':
                LOGGER.debug('Client asked for value for output "%s"' % output_name)
                return False

            # Client provides a dictionary per output, containing transmissionMode:
            elif 'transmissionMode' in requested_outputs[output_name]:
                if requested_outputs[output_name]['transmissionMode'] == 'reference':
                    LOGGER.debug('Asked for reference: %s' % output_name)
                    return True
                elif requested_outputs[output_name]['transmissionMode'] == 'value':
                    LOGGER.debug('Asked for value: %s' % output_name)
                    return False

        LOGGER.debug('Fallback: Returning reference...')
        return True # against specs


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
        #outputs_dict = {
        #    'title': self.metadata['outputs'][output_name]['title'],
        #    'description': self.metadata['outputs'][output_name]['description'],
        #    'href': downloadlink
        #}

        return downloadlink



def call_bash_script(LOGGER, bash_file_name, path_bash_scripts, args):
    # TODO: Move function to some module, same in all processes

    LOGGER.debug('Now calling bash: %s' % bash_file_name)
    bash_file = path_bash_scripts.rstrip('/')+os.sep+bash_file_name
    cmd = [bash_file] + args
    LOGGER.info(cmd)
    LOGGER.debug('Running command... (Output will be shown once finished)')
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate()
    LOGGER.debug("Done running command! Exit code from bash: %s" % p.returncode)

    # Print stdout and stderr
    stdouttext = stdoutdata.decode()
    stderrtext = stderrdata.decode()
    if len(stderrdata) > 0:
        err_and_out = 'Bash stdout and stderr:\n___PROCESS OUTPUT {name} ___\n___stdout___\n{stdout}\n___stderr___\n{stderr}\n___END PROCESS OUTPUT {name} ___\n______________________'.format(
            name=bash_file_name, stdout=stdouttext, stderr=stderrtext)
        LOGGER.error(err_and_out)
    else:
        err_and_out = 'Bash sstdout:\n___PROCESS OUTPUT {name} ___\n___stdout___\n{stdout}\n___stderr___\n___(Nothing written to stderr)___\n___END PROCESS OUTPUT {name} ___\n______________________'.format(
            name=bash_file_name, stdout=stdouttext)
        LOGGER.info(err_and_out)


    # Extract error message from bash output, if applicable:
    err_msg = None
    if not p.returncode == 0:
        err_msg = 'Bash script "%s" failed:' % bash_file_name
        for line in stderrtext.split('\n'):
            if ": line " in line:
                LOGGER.error('FOUND BASH ERROR LINE: %s' % line)
                err_msg += ' line ' + line.split(': line ')[1]
                LOGGER.error('ENTIRE BASH ERROR MSG NOW: %s' % err_msg)
            elif "Error" in line:
                LOGGER.error('FOUND BASH ERROR LINE: %s' % line)
                err_msg += ' ' + line.strip()
                LOGGER.error('ENTIRE BASH ERROR MSG NOW: %s' % err_msg)
            else:
                LOGGER.debug('Seems to be a normal line: %s' % line)

    return p.returncode, stdouttext, stderrtext, err_msg
