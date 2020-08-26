import logging

from datetime import datetime
from simple_salesforce import Salesforce

import config

from sfdc.custom_bulk_client import SFBulkCustomClient
from sfdc.pb_client import ProcessBuilderManager

class SFClient:
    def __init__(self, username: str, password: str, sec_token: str):
        self.client = Salesforce(username=username, password=password, security_token=sec_token)
        self.bulk_client = SFBulkCustomClient(self.client.bulk_url, self.client.session_id)
        self.pb_client = ProcessBuilderManager(self.client)
        self.batch_size_limit = 2000

    @staticmethod
    def from_config():
        return SFClient(config.Salesforce.username, config.Salesforce.password, config.Salesforce.security_token)

    def get_open_opps(self):
        soql = f'SELECT Id FROM Opportunity WHERE IsClosed = False AND >= {datetime.today().year}-01-01'
        logging.info('Downloading Opportunities...')
        response = self.client.query_all(soql)

        return [o['id'] for o in response['records']]

    def get_contacts_by_username(self):
        soql = 'SELECT Id, Username__c, Apex_Bypass_Toggle__c FROM Contact ORDER BY Username__c ASC'

        logging.info('Downloading Contacts from Salesforce...')
        response = self.client.query_all(soql)

        return {u['Username__c'].lower(): u for u in response['records']}

    def update_contacts(self, contacts_for_update: list):
        group_count, remainder = divmod(len(contacts_for_update), self.batch_size_limit)

        for i in range(0, group_count):
            start = self.batch_size_limit * i
            end = self.batch_size_limit * (i + 1) - 1

            if end >= len(contacts_for_update):
                end = len(contacts_for_update) - 1

            logging.info(f'Sending Contact update rows {start} to {end}')
            self.client.update_contacts(contacts_for_update[start:end])

    def update_bulk(self, contacts_for_update: list, object_name):
        self.bulk_client.submit_records(contacts_for_update, sobject=object_name)

    def monitor_job_queue(self):
        self.bulk_client.monitor_job_queue()

    def activate_pb_processes(self, object_name: str='Contact'):
        self.pb_client.toggle_processes(activate=True, sobject=object_name)

    def deactivate_pb_processes(self, object_name: str='Contact'):
        self.pb_client.toggle_processes(activate=False, sobject=object_name)