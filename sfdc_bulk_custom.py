import jsonpickle
import requests
import xmltodict

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
        return requests.post(url=endpoint,
                             headers=self.headers_xml if content_type == 'XML' else self.headers_json,
                             data=payload)

    def create_job_xml(self, operation, sobject):

        xml_str = f"""<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                <operation>{operation}</operation>
                <object>{sobject}</object>
                <contentType>XML</contentType>                
            </jobInfo>"""

        payload = xml_str.encode('utf-8')
        url = f"{self.bulk_url}job"

        response = self.submit(url, payload, 'XML')

        xml_tree = xmltodict.parse(response.text)

        return xml_tree['jobInfo']['id']

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

    def add_batch_xml(self, job_id, object_xml):
        xml_str = f"""<?xml version="1.0" encoding="UTF-8"?>
          <sObjects xmlns="http://www.force.com/2009/06/asyncapi/dataload">{object_xml}
          </sObjects>"""

        payload = xml_str.encode('utf-8')
        url = f"{self.bulk_url}job/{job_id}/batch"

        response = self.submit(url, payload, 'XML')

        xml_tree = xmltodict.parse(response.text)
        return xml_tree['batchInfo']['id']

    def add_batch_json(self, job_id, object_list):
        url = f"{self.bulk_url}job/{job_id}/batch"

        response = self.submit(url, jsonpickle.encode(object_list))

        return response.json()['id']


    def close_job_xml(self, job_id):
        xml_str = u"""<?xml version="1.0" encoding="UTF-8"?>
          <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
            <state>Closed</state>
          </jobInfo>"""

        payload = xml_str.encode('utf-8')
        url = f"{self.bulk_url}job/{job_id}"

        response = self.submit(url, payload, 'XML')

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
        # group_count = 3

        for i in range(0, group_count + 1):
            start = self.batch_record_limit * i
            end = self.batch_record_limit * (i + 1) - 1

            if end >= len(contacts_for_update):
                end = len(contacts_for_update) - 1

            print(f'Sending Contact batch {i + 1} - updating rows {start} to {end}')
            # xml_str =''.join([create_xml_payload(cnt) for cnt in contacts_for_update[start:end]])
            self.add_batch_json(job_id, contacts_for_update[start:end])

        print('Closing batch job...')
        self.close_job_json(job_id)


def create_xml_payload(contact_json):
    sobject = '<sObject>'
    for k, v in contact_json.items():
        sobject += f'<{k}>{v}</{k}>'

    sobject += '</sObject>'

    return sobject




