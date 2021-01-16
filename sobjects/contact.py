import logging
import sys

import bq
import sfdc
import utility

from sfdc import payload
from sobjects import pb

def update_contacts(export_results: bool=False, record_count_limit: int=0):
    pb.disable_process_builder(object_name='Contact')

    update_contact_records(export_results, record_count_limit)

    pb.enable_process_builder(object_name='Contact')


def update_contact_records(export_results: bool=False, record_count_limit: int=0):
    # Obtain data from BigQuery
    query_str = f"SELECT * FROM {bq.bq_client.dataset_id}.{bq.bq_client.tables['contact_table_id']} ORDER BY username"
    bq.bq_client.query(query_str)
    # bq_total = bq_client.results.total_rows

    # Obtain complete Contact list from Salesforce
    contact_list = sfdc.sfdc_client.get_contacts_by_username()

    logging.info('Matching Salesforce Contacts to BigQuery results...')

    contacts_for_update = []
    index = 0
    for row in bq.bq_client.results:
        username = row['username'].lower()

        if username in contact_list:
            sfdc_id = contact_list[username]['Id']
            bypass_toggle = contact_list[username]['Apex_Bypass_Toggle__c']
            contacts_for_update.append(payload.build_contact_payload(sfdc_id, bypass_toggle, row))

        if 0 < record_count_limit <= index:
            break

        index += 1

    dict_size = sys.getsizeof(contacts_for_update)

    logging.info(f'Matched {len(contacts_for_update)} Contacts  ({dict_size:,} bytes). Updating...')

    if export_results:
        utility.export_json(contacts_for_update)
    else:

        sfdc.sfdc_client.update_bulk(contacts_for_update, object_name='Contact')

        sfdc.sfdc_client.monitor_job_queue()

    logging.info('done!')