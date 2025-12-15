import json
import logging
import requests
import urllib
import tempfile
import pandas as pd
from pygeoapi.process.base import ProcessorExecuteError
LOGGER = logging.getLogger(__name__)


def mandatory_parameters(params_dict):
    missing = []
    for paramname, paramval in params_dict.items():
        if paramval is None:
            missing.append(paramname)

    if len(missing) > 0:
        err_msg = f"Missing parameter(s): {', '.join(missing)}"
        raise ProcessorExecuteError(err_msg)


def exactly_one_param(params_dict):
    present = []
    missing = []
    for paramname, paramval in params_dict.items():
        if paramval is None:
            missing.append(paramname)
        else:
            present.append(paramname)

    if len(present) == 0:
        err_msg = f"Missing parameter(s): {', '.join(params_dict.keys())}. Please provide one of them."
        raise ProcessorExecuteError(err_msg)
    elif len(present) > 1:
        err_msg = f"Too many parameter(s): {', '.join(present)}. Please provide just one of them."
        raise ProcessorExecuteError(err_msg)


def return_hyperlink(output_name, requested_outputs):

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

def store_to_json_file(output_name, json_object, job_metadata, job_id, download_dir, download_url):

    # Store to file
    downloadfilename = 'outputs-%s-%s-%s.json' % (output_name, job_metadata['id'], job_id)
    downloadfilepath = download_dir+downloadfilename
    LOGGER.debug('Writing process result to json file: %s' % downloadfilepath)
    with open(downloadfilepath, 'w', encoding='utf-8') as downloadfile:
        json.dump(json_object, downloadfile, ensure_ascii=False, indent=4)

    # Create download link:
    downloadlink = download_url + downloadfilename

    # Create output to pass back to user
    outputs_dict = {
        'title': job_metadata['outputs'][output_name]['title'],
        'description': job_metadata['outputs'][output_name]['description'],
        'href': downloadlink
    }

    return outputs_dict


def store_to_csv_file(output_name, pandas_df, job_metadata, job_id, download_dir, download_url, sep=","):

    # How NaN should be stored in the CSV (if you set nothing, it is a string of length 0)
    store_na='NA'

    # Store to file
    downloadfilename = 'outputs-%s-%s-%s.csv' % (output_name, job_metadata['id'], job_id)
    downloadfilepath = download_dir+downloadfilename
    LOGGER.debug('Writing process result to csv file: %s' % downloadfilepath)
    pandas_df.to_csv(downloadfilepath, sep=sep, encoding='utf-8', index=False, header=True, na_rep=store_na)

    # Create download link:
    downloadlink = download_url + downloadfilename

    # Create output to pass back to user
    outputs_dict = {
        'title': job_metadata['outputs'][output_name]['title'],
        'description': job_metadata['outputs'][output_name]['description'],
        'href': downloadlink
    }

    return outputs_dict


def download_geojson(geojson_url):
    LOGGER.debug(f'Downloading input GeoJSON from: {geojson_url}')

    try:
        resp = requests.get(geojson_url)

    # Files stored on Nimbus: We get SSL error:
    except requests.exceptions.SSLError as e:
        LOGGER.warning(f'SSL error when downloading input data from {geojson_url}: {e}')
        if ('nimbus.igb-berlin.de' in geojson_url and
            'nimbus.igb-berlin.de' in str(e) and
            'certificate verify failed' in str(e)):
            resp = requests.get(geojson_url, verify=False)

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        err_msg = f'Failed to download GeoJSON (HTTP {resp.status_code}) from {geojson_url}.'
        LOGGER.error(err_msg)
        raise exc.DataAccessException(err_msg)

    geojson_content = resp.json()
    return geojson_content


def access_csv_comma_then_semicolon(csv_url_or_path):

    # Try with a comma separator first:
    dataframe = pd.read_csv(csv_url_or_path)

    # If that failed, try semicolon:
    if dataframe.shape[1] == 1:
        LOGGER.debug(f'Found only one column (name "{dataframe.columns}"). Maybe it is not comma-separated, but comma-separated? Trying...')
        dataframe = pd.read_csv(csv_url_or_path, sep=';')

    return dataframe


def access_csv_as_dataframe(csv_url_or_path):
    LOGGER.debug(f'Accessing input CSV from: {csv_url_or_path}')

    try:
        input_df = access_csv_comma_then_semicolon(csv_url_or_path)
        LOGGER.debug('Accessing input CSV... Done.')

    except urllib.error.URLError as e:
        LOGGER.warning(f'SSL error when downloading input CSV from {csv_url_or_path}: {e}')

        # Files stored on Nimbus: We get SSL error:
        if ('nimbus.igb-berlin.de' in csv_url_or_path and
            'certificate verify failed' in str(e)):
            LOGGER.debug('Will download input CSV with verify=False to a tempfile.')
            resp = requests.get(csv_url_or_path, verify=False)
            resp.raise_for_status()

            mytempfile = tempfile.NamedTemporaryFile()
            mytempfile.write(resp.content)
            mytempfile.flush()
            mytempfilename = mytempfile.name
            LOGGER.debug(f'CSV file stored to tempfile successfully: {mytempfilename}')

            input_df = access_csv_comma_then_semicolon(mytempfilename)
            LOGGER.debug('Accessing input CSV... Done.')
            mytempfile.close()

    return input_df


