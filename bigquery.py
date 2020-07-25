import csv

from datetime import datetime
from google.cloud import bigquery
from pathlib import Path

import config

from utility import time_query

class BQClient:
    def __init__(self, cred_path: str, project_id: str, dataset_id: str, table_id: str):
        self.cred_path = cred_path
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.query_client = bigquery.Client.from_service_account_json(self.cred_path)
        self.last_query = ''
        self.results = None

    @staticmethod
    def from_config():
        return BQClient(config.Google.credentials,
                        config.Google.project_id,
                        config.Google.dataset_id,
                        config.Google.table_id)

    def query(self, query_str: str=None):
        if not query_str:
            query_str = self.last_query

        if query_str:
            self.last_query = query_str

            print('Executing query...')
            self.__execute_query()

    def export_csv(self):
        filename = f'bq_export_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.csv'
        export_path = Path('./export', filename)

        print('Exporting results to CSV...')

        with open(export_path, 'w') as export_file:
            #field_names = [s.name for s in self.results._query_results.schema]

            field_names = ['username', 'last_login_timestamp', 'documents_read_count', 'watchlists_created_count',
                           'watchlists_deleted_count', 'watchlists_modified_count', 'net_watchlists_created',
                           'alert_creation_count', 'alert_deletion_count', 'net_alert_creation_count']

            # restval specifies what value to write if the row being written doesn't have a key for a given fieldname
            # extrasaction specifies what to do if the row being written has keys not found in fieldnames
            # lineterminator tells the writer to only use newline characters after each line; otherwise, it inserts
            #   extra lines between each data row.
            csv_writer = csv.DictWriter(export_file, fieldnames=field_names, restval='', extrasaction='ignore',
                                        lineterminator='\n')
            csv_writer.writeheader()

            index = 0
            for row in self.results:
                index += 1
                csv_writer.writerow(dict(row.items()))
                # This didn't work
                # print(f'\rLines written: {index}')

        print('done!')

    @time_query
    def __execute_query(self):
        self.results = self.query_client.query(self.last_query).result()