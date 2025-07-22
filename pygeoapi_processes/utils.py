import json
import logging
LOGGER = logging.getLogger(__name__)

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
