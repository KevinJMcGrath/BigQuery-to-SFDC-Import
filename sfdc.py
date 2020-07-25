from simple_salesforce import Salesforce

import config

class SFClient:
    def __init__(self, username: str, password: str, sec_token: str):
        self.client = Salesforce(username=username, password=password, security_token=sec_token)
        self.batch_size_limit = 2000

    @staticmethod
    def from_config():
        return SFClient(config.Salesforce.username, config.Salesforce.password, config.Salesforce.security_token)

    def get_contacts_by_username(self):
        soql = 'SELECT Id, Username__c, Apex_Bypass_Toggle__c FROM Contact ORDER BY Username__c ASC'

        print('Downloading Contacts from Salesforce...', end='')
        response = self.client.query_all(soql)
        print('done!')

        return {u['Username__c']: u for u in response['records']}

    def update_contacts(self, contacts_for_update: list):
        group_count, remainder = divmod(len(contacts_for_update), self.batch_size_limit)

        for i in range(0, group_count):
            start = self.batch_size_limit * i
            end = self.batch_size_limit * (i + 1) - 1

            if end >= len(contacts_for_update):
                end = len(contacts_for_update) - 1

            print(f'Sending Contact update rows {start} to {end}')
            self.client.update_contacts(contacts_for_update[start:end])