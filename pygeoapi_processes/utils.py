import json
import requests
import urllib
import tempfile
import pandas as pd
from pygeoapi.process.base import ProcessorExecuteError
import pygeoapi.process.aqua90m.utils.exceptions as exc
import pygeoapi.process.aqua90m.utils.geojson_helpers as geojson_helpers

import logging
logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
LOGGER = logging.getLogger(__name__)


def params_lonlat_or_subcid(lon, lat, subc_id, additional_message=""):

    # subc_id takes precedence:
    if subc_id is not None:
        if not isinstance(subc_id, int):
            err_msg = (
                f"Malformed parameter: 'subc_id' has to be integer,"
                f" not {type(subc_id).__name__}!{additional_message}"
            )
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

    # lon, lat comes after:
    elif lon is not None and lat is not None:
        if not (isinstance(lon, float) and isinstance(lon, float)):
            err_msg = (
                f"Malformed parameter: Both 'lon' and 'lat' have to be decimal numbers,"
                f" not '{type(lon).__name__}' and '{type(lat).__name__}'."
                f"{additional_message}"
            )
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

    # errors:
    elif (lon is None) and (lat is None):
        err_msg = (
            f"Missing parameter: Please provide either 'subc_id' or both 'lon' and 'lat'!"
            f"{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)

    else:
        err_msg = (
            f"Missing parameter: Please provide both lon and lat!"
            f"{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)


def params_point_or_lonlat_or_subcid(point, lon, lat, subc_id, additional_message=""):

    # subc_id takes precedence:
    if subc_id is not None:
        if not isinstance(subc_id, int):
            err_msg = (
                f"Malformed parameter: 'subc_id' has to be integer,"
                f" not {type(subc_id).__name__}!{additional_message}"
            )
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

    # point is second:
    elif point is not None:
        geojson_helpers.check_is_geojson(point)
        if point['type'] == 'Point':
            pass
        elif point['type'] == 'Feature' and point['geometry']['type'] == 'Point':
            pass
        else:
            geojson_type = point['type']
            if geojson_type == 'Feature':
                geojson_type = point['geometry']['type']
            err_msg = (
                f"Malformed parameter: 'point' has to be a GeoJSON point,"
                f" not {geojson_type}!{additional_message}"
            )
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

    # lon, lat comes after:
    elif lon is not None and lat is not None:
        if not (isinstance(lon, float) and isinstance(lon, float)):
            err_msg = (
                f"Malformed parameter: Both 'lon' and 'lat' have to be decimal numbers,"
                f" not '{type(lon).__name__}' and '{type(lat).__name__}'."
                f"{additional_message}"
            )
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)

    # errors:
    elif (lon is None) and (lat is None):
        err_msg = (
            f"Missing parameter: Please provide either 'point' or 'subc_id' or both 'lon' and 'lat'!"
            f"{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)

    else:
        err_msg = (
            f"Missing parameter: Please provide both lon and lat!"
            f"{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)


def mandatory_parameters(params_dict, additional_message=""):
    LOGGER.debug(f'Mandatory params: {params_dict.keys()}')
    missing = []
    for paramname, paramval in params_dict.items():
        if paramval is None:
            missing.append(paramname)

    if len(missing) > 0:
        err_msg = (
            f"Missing parameter(s): {', '.join(missing)}."
            f" Please provide all of them.{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)


def exactly_one_param(params_dict, additional_message=""):
    LOGGER.debug(f'Exactly one is mandatory: {params_dict.keys()}')
    present = []
    for paramname, paramval in params_dict.items():
        if paramval is None:
            LOGGER.debug(f'Absent:  {paramname}')
        else:
            LOGGER.debug(f'Present: {paramname}')
            present.append(paramname)

    if len(present) == 0:
        err_msg = (
            f"Missing parameter(s): {', '.join(params_dict.keys())}."
            f" Please provide exactly one of them.{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)
    elif len(present) > 1:
        err_msg = (
            f"Too many parameter(s): {', '.join(present)}."
            f" Please provide just one of them.{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)


def at_least_one_param(params_dict, additional_message=""):
    LOGGER.debug(f'At least one is mandatory: {params_dict.keys()}')
    present = []
    for paramname, paramval in params_dict.items():
        if paramval is None:
            LOGGER.debug(f'Absent:  {paramname}')
        else:
            LOGGER.debug(f'Present: {paramname}')
            present.append(paramname)

    if len(present) == 0:
        err_msg = (
            f"Missing parameter(s): {', '.join(params_dict.keys())}."
            f" Please provide at least one of them.{additional_message}"
        )
        LOGGER.error(err_msg)
        raise ProcessorExecuteError(err_msg)


def is_bool_parameters(params_dict, additional_message=""):
    LOGGER.log(logging.TRACE, f'Checking parameters: All of these should be boolean: {params_dict.keys()}')
    for paramname, paramval in params_dict.items():
        if not type(paramval) == bool:
            err_msg = (
                f"Malformed parameter: '{paramname}' should be a 'boolean' "
                f"instead of '{type(paramval).__name__}'.{additional_message}"
            )
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)


def check_type_parameter(paramname, paramval, paramtype, none_allowed=False, additional_message=""):
    LOGGER.log(logging.TRACE, f'Checking parameter {paramname}...')
    if none_allowed and paramval is None:
        pass
    elif not type(paramval) == paramtype:
        err_msg = (
            f"Malformed parameter: '{paramname}' should be a '{paramtype.__name__}' "
            f"instead of '{type(paramval).__name__}'.{additional_message}"
        )
        LOGGER.error(err_msg)
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
    process_id = job_metadata['id']
    downloadfilename = f'outputs-{output_name}-{process_id}-{job_id}.json'
    downloadfilepath = download_dir+downloadfilename
    LOGGER.debug(f'Writing process result to json file: {downloadfilepath}')
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
    # TODO: How to store NaN and NA to csv? pandas...

    # Store to file
    process_id = job_metadata['id']
    downloadfilename = f'outputs-{output_name}-{process_id}-{job_id}.csv'
    downloadfilepath = download_dir+downloadfilename
    LOGGER.debug(f'Writing process result to csv file: {downloadfilepath}')
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


def download_json(json_url):
    LOGGER.debug(f'Downloading input JSON from: {json_url}')
    return _download_json(json_url)


def download_geojson(geojson_url):
    LOGGER.debug(f'Downloading input GeoJSON from: {geojson_url}')
    return _download_json(geojson_url)


def _download_json(json_url):

    try:
        resp = requests.get(json_url)

    # Files stored on Nimbus: We get SSL error:
    except requests.exceptions.SSLError as e:
        LOGGER.warning(f'SSL error when downloading input data from {json_url}: {e}')
        if ('nimbus.igb-berlin.de' in json_url and
            'nimbus.igb-berlin.de' in str(e) and
            'certificate verify failed' in str(e)):
            resp = requests.get(json_url, verify=False)

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        err_msg = f'Failed to download JSON (HTTP {resp.status_code}) from {json_url}.'
        LOGGER.error(err_msg)
        raise exc.DataAccessException(err_msg)

    json_content = resp.json()
    return json_content


def access_csv_comma_then_semicolon(csv_url_or_path):

    # Try with a comma separator first:
    dataframe = pd.read_csv(csv_url_or_path)

    # If that failed, try semicolon:
    if dataframe.shape[1] == 1:
        LOGGER.debug(
            f'Found only one column (name "{dataframe.columns}").'
            f' Maybe it is not comma-separated, but comma-separated? Trying...'
        )
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


def _split_df(input_df, num_rows_per_chunk):
    # returns a generator
    for i in range(0, len(input_df), num_rows_per_chunk):
        yield input_df.iloc[i:i + num_rows_per_chunk]

def access_csv_as_dataframe_iterator(csv_url_or_path, num_rows_per_chunk):
    input_df = access_csv_as_dataframe(csv_url_or_path)
    generator = _split_df(input_df, num_rows_per_chunk)
    return generator
