import jsonpickle
import logging
import requests
import time

from datetime import datetime, timedelta

import utility

# http://www.wadewegner.com/2014/04/update-records-with-python-and-the-salesforce-bulk-api/
class SFBulkCustomClient:
    def __init__(self, sfdc_bulk_url: str, session_id: str):
        self.bulk_url = sfdc_bulk_url
        self.session_id = session_id
        self.api_version = '50.0'
        self.batch_record_limit = 4000

        self.headers_xml = {"X-SFDC-Session": self.session_id, "Content-Type": "application/xml; charset=UTF-8"}
        self.headers_json = {"X-SFDC-Session": self.session_id, "Content-Type": "application/json"}

        self.job_queue = {}

    def submit(self, endpoint, payload, content_type: str='JSON'):
        return requests.post(url=endpoint, headers=self.headers_json, data=payload)

    def get(self, endpoint):
        return requests.get(url=endpoint, headers=self.headers_json).json()

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

    def get_job_results(self, job_id: str):
        retval = {}

        url = f"{self.bulk_url}job/{job_id}/batch"
        resp = self.get(url)

        for batch in resp['batchInfo']:
            batch_id = batch['id']

            batch_res_url = f"{self.bulk_url}job/{job_id}/batch/{batch_id}/result"

            batch_res = self.get(batch_res_url)

            retval.update(batch_res)

        return retval


    def get_batch_results(self, job_id: str, batch_id: str):
        url = f"{self.bulk_url}job/{job_id}/batch/{batch_id}/result"

        response = requests.get(url=url, headers=self.headers_json)

        return response.json()

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

    def send_bulk_insert(self, records_for_insert: list, sobject: str='Contact'):
        return self.send_bulk_operation('insert', records_for_insert, sobject)

    def send_bulk_update(self, records_for_update: list, sobject: str='Contact'):
        return self.send_bulk_operation('update', records_for_update, sobject)

    def send_bulk_operation(self, operation_type: str, records_for_update: list, sobject: str='Contact'):
        # create the batch job
        job_id = self.create_job_json(operation_type, sobject=sobject)

        job = None
        if job_id not in self.job_queue:
            job = {
                "id": job_id,
                "is_complete": 0,
                "batch_total": 0,
                "batch_count": 0,
                "batch_complete_count": 0,
                "records_total": 0,
                "records_processed": 0,
                "records_failed": 0
            }
        else:
            job = self.job_queue.get(job_id)

        job['records_total'] += len(records_for_update)


        if len(records_for_update) > self.batch_record_limit:
            # add batches to the job
            group_count, remainder = divmod(len(records_for_update), self.batch_record_limit)

            for i in range(0, group_count + 1):
                start = self.batch_record_limit * i
                end = self.batch_record_limit * (i + 1) - 1

                job['batch_total'] += 1

                logging.info(f'Sending {sobject} batch {i + 1} - updating {len(records_for_update[start:end])} rows.')
                self.add_batch_json(job_id, records_for_update[start:end])
        else:
            logging.info(f'Sending single {sobject} batch to job')
            job['batch_total'] = 1
            self.add_batch_json(job_id, records_for_update)

        logging.info('Closing job...')
        self.job_queue[job_id] = job
        self.close_job_json(job_id)

        return job_id

    def update_job_status(self, job_id: str):
        url = f"{self.bulk_url}job/{job_id}/batch"

        resp = requests.get(url, headers=self.headers_json).json()

        job_is_complete = True
        job_records_processed = 0
        job_records_failed = 0
        job_batch_count_total = 0
        job_batch_complete_total = 0

        for batch in resp['batchInfo']:
            batch_id = batch['id']
            batch_state = batch['state']
            batch_status = batch_state == 'Completed' or batch_state == 'Failed'
            batch_records_processed = batch['numberRecordsProcessed']
            batch_records_failed = batch['numberRecordsFailed']

            job_batch_count_total += 1
            job_records_processed += batch_records_processed
            job_records_failed += batch_records_failed

            if batch_status:
                job_batch_complete_total += 1

            if 'Fail' in batch_state:
                logging.error(f'Failure detected for job.batch: {job_id}.{batch_id}.')
                return True

            logging.debug(f"batch id: {batch_id} - state: {batch_state} - "
                         f"records processed: {batch_records_processed} - "
                         f"records failed: {batch_records_failed}")

            job_is_complete = job_is_complete and batch_status

        job_status = self.job_queue.get(job_id)
        job_status['is_complete'] = job_is_complete
        job_status['batch_count'] = job_batch_count_total
        job_status['batch_complete_count'] = job_batch_complete_total
        job_status['records_processed'] = job_records_processed
        job_status['records_failed'] = job_records_failed

    def monitor_job_queue(self):
        job_completed_count = 0
        queue_length = len(self.job_queue)

        # Only allow the monitor to run for 10 minutes.
        start_d = datetime.now()
        run_d = datetime.now()
        while job_completed_count < queue_length:
            job_count = 1

            for job_id, status in self.job_queue.items():
                self.update_job_status(job_id)

                r_t = status['records_total']
                r_p = status['records_processed']
                r_f = status['records_failed']
                b_t = status['batch_total']
                b_c = status['batch_count']
                b_cp = status['batch_complete_count']

                logging.info(f'Job: {job_count}/{queue_length} - batches: {b_cp}/{b_t} - records: {r_p}/{r_t} ({r_f})')

                if status['is_complete']:
                    job_completed_count += 1

                job_count += 1


            time.sleep(15)
            run_d = datetime.now()

            if run_d > start_d + timedelta(minutes=15):
                logging.error('Monitoring halted due to timeout')
                break

        logging.info('All jobs complete.')
        self.job_queue.clear()