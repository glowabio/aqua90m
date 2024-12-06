
import logging
import traceback
import os
import json
import uuid
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


'''
curl -X POST --location 'http://localhost:5000/processes/get-ddas-galaxy-link-textfile/execution' \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "link_from_ddas": "https://www.bla.com"
    }
}'

'''

LOGGER = logging.getLogger(__name__)

# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class HelferleinProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True
        self.job_id = None
        self.config = None

        # Get config
        config_file_path = os.environ.get('AQUA90M_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            self.config = json.load(config_file)

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<HelferleinProcessor> {self.name}'

    def execute(self, data, outputs=None):
        LOGGER.info('Starting to generate that text file that Galaxy wants..."')
        LOGGER.info('Inputs: %s' % data)
        LOGGER.info('Requested outputs: %s' % outputs)

        try:
            res = self._execute(data, outputs)
            return res

        except Exception as e:
            LOGGER.error('During process execution, this happened: %s' % e)
            print(traceback.format_exc())
            raise ProcessorExecuteError(e) # TODO: Can we feed e into ProcessExecuteError?

    def _execute(self, data, requested_outputs):
        LOGGER.debug('Content of requested_outputs: %s' % requested_outputs)

        # User inputs:
        link_from_ddas = data.get('link_from_ddas')

        # Filename:
        LOGGER.debug('Getting a checksum for: %s' % link_from_ddas)
        identifier = uuid.uuid3(uuid.NAMESPACE_URL, link_from_ddas)
        LOGGER.debug('Got a checksum: %s' % identifier)
        textfile_name = 'link_to_%s.txt' % identifier
        textfile_path = self.config['textfile_directory']+os.sep+textfile_name
        textfile_url = self.config['textfile_download_url'].rstrip('/')+'/'+textfile_name

        # Store link
        if os.path.exists(textfile_path):
            LOGGER.info('Already exists: %s' % textfile_path)
            with open(textfile_path, 'r') as myfile:
                for line in myfile:
                    LOGGER.info('CONTENT: %s' % line)
                    # TODO: Should be only one line, and should be same as link...
                    # TODO WIP CHECK CONTENT
                LOGGER.debug('Finished for loop...')
            LOGGER.debug('Closed file again I hope...')
        else:
            with open(textfile_path, 'w') as textfile:
                LOGGER.info('Writing into text file: %s' % link_from_ddas)
                textfile.write(link_from_ddas)
                LOGGER.info('Done writing into text file: %s' % textfile_path)
                # TODO: Must delete result files periodically.
            LOGGER.debug('Closed file again I hope...')

        LOGGER.debug('Link to be returned: %s' % textfile_url)

        outputs = {
            "textfile": {
                "title": PROCESS_METADATA["outputs"]["textfile"]["title"],
                "description": PROCESS_METADATA["outputs"]["textfile"]["description"],
                "link_from_ddas": link_from_ddas,
                "href": textfile_url
            }
        }
        LOGGER.debug('Will return: %s' % outputs)
        return 'application/json', outputs


