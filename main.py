import logging
import time
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from optparse import OptionParser

import package_logger
import utility

from bigquery import BQClient
from sfdc import payload
from sfdc.client import SFClient

package_logger.initialize_logging()

bq_client = BQClient.from_config()
sfdc_client = SFClient.from_config()

def run_main():
    update_contacts()
    update_opps()

def update_opps():
    query_str = f"SELECT * FROM {bq_client.dataset_id}.{bq_client.tables['opp_table_id']}"
    bq_client.query(query_str)

    # They're giving me the opp Id. I don't really need to match them.
    # opp_ids = sfdc_client.get_open_opps()

    opps_for_update = []

    for row in bq_client.results:
        opps_for_update.append(payload.build_opp_payload(row))

    # Disable Process Builder processes
    sfdc_client.deactivate_pb_processes(object_name='Opportunity')

    sfdc_client.update_bulk(opps_for_update, object_name='Opportunity')

    sfdc_client.monitor_job_queue()

    sfdc_client.activate_pb_processes(object_name='Opportunity')

    logging.info('done!')

def update_contacts(export_results: bool=False, record_count_limit: int=0):

    # Obtain data from BigQuery
    query_str = f"SELECT * FROM {bq_client.dataset_id}.{bq_client.tables['contact_table_id']} ORDER BY username"
    bq_client.query(query_str)
    bq_total = bq_client.results.total_rows

    # Obtain complete Contact list from Salesforce
    contact_list = sfdc_client.get_contacts_by_username()

    logging.info('Matching Salesforce Contacts to BigQuery results...')

    contacts_for_update = []
    index = 0
    for row in bq_client.results:
        username = row['username']

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
        # Disable Process Builder processes
        sfdc_client.deactivate_pb_processes(object_name='Contact')

        sfdc_client.update_bulk(contacts_for_update, object_name='Contact')

        sfdc_client.monitor_job_queue()

        sfdc_client.activate_pb_processes(object_name='Contact')

    logging.info('done!')


def run_sched():
    scheduler = BlockingScheduler()
    scheduler.add_job(run_main, 'cron', hour=1)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit) as ex:
        logging.info(f'Exiting process...{ex}')


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-j", "--headless", help="Run system on an unattended schedule", dest="headless_flag",
                      default=None, action="store_true")

    parser.add_option("-O", "--opps", help="Manually execute import for Opportunity data.", dest="opps_flag",
                      default=None, action="store_true")

    parser.add_option("-C", "--contacts", help="Manually execute import for Contact data.", dest="cnts_flag",
                      default=None, action="store_true")

    options, args = parser.parse_args()

    if options.headless_flag:
        run_sched()
    elif options.opps_flag:
        update_opps()
    elif options.cnts_flagg:
        update_contacts()
    else:
        run_main()