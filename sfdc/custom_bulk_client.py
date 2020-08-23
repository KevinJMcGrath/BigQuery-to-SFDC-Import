import jsonpickle
import logging
import requests
import time

import utility

# http://www.wadewegner.com/2014/04/update-records-with-python-and-the-salesforce-bulk-api/
class SFBulkCustomClient:
    def __init__(self, sfdc_bulk_url: str, session_id: str):
        self.bulk_url = sfdc_bulk_url
        self.session_id = session_id
        self.api_version = '48.0'
        self.batch_record_limit = 4800

        self.headers_xml = {"X-SFDC-Session": self.session_id, "Content-Type": "application/xml; charset=UTF-8"}
        self.headers_json = {"X-SFDC-Session": self.session_id, "Content-Type": "application/json"}

        self.job_queue = {}

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

    def submit_records(self, records_for_update: list, sobject: str='Contact', use_multiple_jobs: bool=False):
        if use_multiple_jobs:
            records_per_job, groups_per_job = utility.get_job_parameters(records_for_update)

            for i in range(0, groups_per_job):
                start = records_per_job * i
                end = records_per_job * (i + 1)

                logging.info(f'Sending {sobject} job {i + 1} - updating {len(records_for_update[start:end])} rows.')
                self.send_bulk_update(records_for_update[start:end], sobject=sobject)
        else:
            self.send_bulk_update(records_for_update, sobject=sobject)

    def send_bulk_update(self, records_for_update: list, sobject: str='Contact'):
        # create the batch job
        job_id = self.create_job_json('update', sobject=sobject)
        self.job_queue[job_id] = False

        if len(records_for_update) > self.batch_record_limit:
            # add batches to the job
            group_count, remainder = divmod(len(records_for_update), self.batch_record_limit)

            for i in range(0, group_count + 1):
                start = self.batch_record_limit * i
                end = self.batch_record_limit * (i + 1) - 1

                logging.info(f'Sending {sobject} batch {i + 1} - updating {len(records_for_update[start:end])} rows.')
                self.add_batch_json(job_id, records_for_update[start:end])
        else:
            logging.info(f'Sending single {sobject} batch to job')
            self.add_batch_json(job_id, records_for_update)

        logging.info('Closing job...')
        self.close_job_json(job_id)

    def check_bulk_job_complete(self, job_id: str):
        url = f"{self.bulk_url}job/{job_id}/batch"

        resp = requests.get(url, headers=self.headers_json).json()

        is_complete = True
        for batch in resp['batchInfo']:
            batch_id = batch['id']
            batch_status = batch['state'] == 'Completed' or batch['state'] == 'Failed'

            if 'Fail' in batch['state']:
                logging.error(f'Failure detected for job.batch: {job_id}.{batch_id}.')
                return True

            logging.debug(f"batch id: {batch['id']} - state: {batch['state']} - "
                         f"records processed: {batch['numberRecordsProcessed']} - "
                         f"records failed: {batch['numberRecordsFailed']}")
            is_complete = is_complete and batch_status

        return is_complete

    def monitor_job_queue(self):
        pending_count = 0
        queue_length = len(self.job_queue)

        while pending_count < queue_length:
            for job_id, status in self.job_queue.items():
                if not status:
                    job_status = self.check_bulk_job_complete(job_id)

                    if job_status:
                        pending_count += 1
                        self.job_queue[job_id] = job_status

            logging.info(f'{pending_count} of {queue_length} remaining.')
            time.sleep(15)

        logging.info('All jobs complete.')