
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
curl -X POST "http://localhost:5000/processes/extract-point-stats/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lonlatstring\": \"lon lat\n5.5 52.7\n7.3 51.6\", \"variable\": \"sti\", \"comment\":\"schna\", \"colname_lon\":\"lon\", \"colname_lat\":\"lat\"}, \"outputs\": {\"transmissionMode\": \"reference\", \"csv\": \"dummy\", \"geojson\": \"dummy\"}}"

curl -X POST "http://localhost:5000/processes/extract-point-stats/execution" -H "Content-Type: application/json" -d "{\"inputs\":{\"lonlatstring\": \"lon lat\n5.5 52.7\n7.3 51.6\", \"variable\": \"sti\", \"comment\":\"schna\", \"colname_lon\":\"lon\", \"colname_lat\":\"lat\"}, \"outputs\": {\"transmissionMode\": \"value\", \"csv\": \"dummy\", \"geojson\": \"dummy\"}}"

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

        try:
            res = self._execute(data, outputs)
            return res

        except Exception as e:
            LOGGER.error('During process execution, this happened: %s' % e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?


    def _execute(self, data, requested_outputs):

        # User inputs
        # TODO: Allow user passing csv, GeoJSON, ...
        lonlatstring = data.get('lonlatstring', None)
        variable = data.get('variable')  # names, e.g. "spi", "sti" --> enum!
        comment = data.get('comment') # optional
        colname_lat = data.get('colname_lat', None)
        colname_lon = data.get('colname_lon', None)
        points_geojson = data.get('points_geojson', None)

        # Check user inputs
        if variable is None:
            raise ProcessorExecuteError('Need to provide "variable".')
        if lonlatstring is None and points_geojson is None:
            raise ProcessorExecuteError('Need to provide "lonlatstring" or "points_geojson".')
        elif lonlatstring is not None and points_geojson is not None:
            raise ProcessorExecuteError('Need to provide either "lonlatstring" or "points_geojson", not both.')
        elif lonlatstring is not None:
            if colname_lat is None or colname_lon is None:
                 raise ProcessorExecuteError('If you provide "lonlatstring", you also must provide "colname_lat" and "colname_lon".')



        # Write user-provided points to tmp
        # (as input for gdallocation info):
        coord_tmp_path = '/tmp/inputcoordinates_%s.txt' % self.job_id

        # User provided points as feature collection:
        if points_geojson is not None:
            LOGGER.debug('Client provided a GeoJSON FeatureCollection...')
            # TODO Should we validate it?
            with open(coord_tmp_path, 'w') as myfile:
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

        LOGGER.debug('Requested variable "%s"...' % variable)

        # Read path from config:
        path_tiffs = self.config['path_hy90m_rasters'].rstrip('/')

        # EITHER: Path and filename are always the same, given the variable name:
        # TODO: Test case, individual tiles, no VRT yet, so file id has to be specified!
        var_layer = '{path}/{var}_{tile}.tif'.format(
            path = path_tiffs, var=variable, tile='18v02')

        # OR: Path and filename are read from this Lookup Table:
        # TODO: Have an entire Lookup Table stored somewhere? Maybe in config?
        lookup_vrt = {
            "basin": "https://2007367-nextcloud.a3s.fi/igb/vrt/basin.vrt",
            "sti": path_tiffs+'/sti_h18v02.tif',
            "cti": "not-yet"
        }
        var_layer = lookup_vrt[variable]
        LOGGER.debug('Requested variable file "%s"...' % var_layer)

        # Where to store results:
        out_dir = self.config['download_dir'].rstrip('/')
        out_path_txt = out_dir+'/outputs_%s_%s_%s.txt' % (self.metadata['id'], variable, self.job_id)
        out_path_csv = out_dir+'/outputs_%s_%s_%s.csv' % (self.metadata['id'], variable, self.job_id)
        LOGGER.debug('Will write final result here: %s' % out_path_txt)

        # Run bash script
        path_bash_scripts = self.config['hydrographr_bash_files']
        args = [coord_tmp_path, colname_lon, colname_lat, var_layer, variable, out_dir, out_path_txt]
        returncode, stdouttext, stderrtext, err_msg = call_bash_script(LOGGER, "extract_point_stats.sh", path_bash_scripts, args)
        if not returncode == 0:
            raise ProcessorExecuteError(user_msg = err_msg)

        ################
        ### Results: ###
        ################

        outputs = {}

        if "geojson" in requested_outputs.keys() or "ALL" in requested_outputs.keys():

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
                        variable: row[variable]
                    }
                }

            geojson_features = df.apply(create_geojson_feature, axis=1).tolist()
            geojson = {
                "type": "FeatureCollection",
                "features": geojson_features
            }

            if self.return_hyperlink("geojson", requested_outputs):

                downloadlink = self.store_to_json_file("geojson", geojson)
                outputs["geojson"] = {
                    "title": "",
                    "description": "",
                    "value": downloadlink
                }
            else:
                outputs["geojson"] = {
                    "title": "",
                    "description": "",
                    "value": geojson
                }


        if "csv" in requested_outputs.keys() or "ALL" in requested_outputs.keys():

            if self.return_hyperlink("csv", requested_outputs):

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

                outputs["csv"] = {
                    "title": "",
                    "description": "",
                    "value": downloadlink
                }

            else:

                resultstring = ''
                # Transform from space-separated to semicolon-separated:
                with open(out_path_txt, 'r') as txtfile:
                    with open(out_path_csv, 'w') as csvfile:
                        line = txtfile.read().replace(' ', ';')
                        resultstring += line+'\n'

                outputs["csv"] = {
                    "title": "",
                    "description": "",
                    "value": resultstring.rstrip("\n")
                }

        if comment is not None:
            outputs['comment'] = comment

        return 'application/json', outputs


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
        err_and_out = 'Bash sstdour:\n___PROCESS OUTPUT {name} ___\n___stdout___\n{stdout}\n___stderr___\n___(Nothing written to stderr)___\n___END PROCESS OUTPUT {name} ___\n______________________'.format(
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
