import jsonpickle
import logging
import urllib.parse

import config

class ProcessBuilderManager:
    def __init__(self, sfdc_client):
        self.client = sfdc_client

    def query_tooling_api(self, query):
        cleaned_query = urllib.parse.quote_plus(query)
        data = self.client.restful(path=f'tooling/query/?q={cleaned_query}')
        return data

    def get_all_pb_processes(self):
        query = 'Select Id,ActiveVersion.VersionNumber,LatestVersion.VersionNumber,DeveloperName From FlowDefinition'
        response = self.query_tooling_api(query)

        return {pb['Id']: pb for pb in response['records']}

    def submit_toggle_pb_process(self, process_id, version_num=None):
        pb = {
            'Metadata': {
                'activeVersionNumber': version_num
            }
        }

        pb_str = jsonpickle.encode(pb, unpicklable=False)
        response = None

        try:
            # The response coming from Salesforce is apparently malformed and fails to parse properly
            response = self.client.restful(path=f'tooling/sobjects/FlowDefinition/{process_id}/', method='PATCH', data=pb_str)
        except Exception as ex:
            # The tooling API always returns an error for this for some reason. Ignore it
            if 'Expecting value' not in ex:
                logging.error(ex)


    def toggle_processes(self, activate: bool=False):
        pb_map = self.get_all_pb_processes()

        for pb_id, pb in pb_map.items():
            pb_id = pb['Id']
            pb_name = pb['DeveloperName']

            if pb_name in config.Salesforce.process_collection.keys():
                active_version = None

                if activate:
                    active_version = config.Salesforce.process_collection[pb_name]

                logging.info(f'{"Activating" if activate else "Deactivating"} process {pb_name}')
                self.submit_toggle_pb_process(pb_id, active_version)
