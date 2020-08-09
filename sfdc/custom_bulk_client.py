import jsonpickle
import requests

# http://www.wadewegner.com/2014/04/update-records-with-python-and-the-salesforce-bulk-api/
class SFBulkCustomClient:
    def __init__(self, sfdc_bulk_url: str, session_id: str):
        self.bulk_url = sfdc_bulk_url
        self.session_id = session_id
        self.api_version = '48.0'
        self.batch_record_limit = 6000

        self.headers_xml = {"X-SFDC-Session": self.session_id, "Content-Type": "application/xml; charset=UTF-8"}
        self.headers_json = {"X-SFDC-Session": self.session_id, "Content-Type": "application/json"}

    def submit(self, endpoint, payload, content_type: str='JSON'):
        return requests.post(url=endpoint, headers=self.headers_json, data=payload)

    def create_job_json(self, operation: str, sobject):
        # concurrencyMode is the default, does not need to be specified
        payload = {
            "operation": operation.lower(),
            "object": sobject,
            "contentType": "JSON"
        }

        url = f"{self.bulk_url}job"

        payload_str = jsonpickle.encode(payload)

        response = self.submit(url, payload_str)

        return response.json()['id']

    def add_batch_json(self, job_id, object_list):
        url = f"{self.bulk_url}job/{job_id}/batch"

        response = self.submit(url, jsonpickle.encode(object_list))

        return response.json()['id']

    def close_job_json(self, job_id):
        payload = {
            "state": "Closed"
        }

        url = f"{self.bulk_url}job/{job_id}"

        response = self.submit(url, jsonpickle.encode(payload))

    def send_bulk_update(self, contacts_for_update: list):
        # create the batch job
        job_id = self.create_job_json('update', 'Contact')

        # add batches to the job
        group_count, remainder = divmod(len(contacts_for_update), self.batch_record_limit)

        for i in range(0, group_count + 1):
            start = self.batch_record_limit * i
            end = self.batch_record_limit * (i + 1) - 1

            if end >= len(contacts_for_update):
                end = len(contacts_for_update) - 1

            print(f'Sending Contact batch {i + 1} - updating rows {start} to {end}')
            self.add_batch_json(job_id, contacts_for_update[start:end])

        print('Closing batch job...')
        self.close_job_json(job_id)
