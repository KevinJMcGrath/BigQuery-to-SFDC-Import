import logging
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from optparse import OptionParser

import package_logger

from bigquery import BQClient
from sfdc import payload
from sfdc.client import SFClient

package_logger.initialize_logging()

def run_main():
    bq_client = BQClient.from_config()
    sfdc_client = SFClient.from_config()

    # Obtain data from BigQuery
    query_str = f'SELECT * FROM {bq_client.dataset_id}.{bq_client.table_id} ORDER BY username'
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
            contacts_for_update.append(payload.build_payload(sfdc_id, bypass_toggle, row))

        index += 1

    logging.info(f'Matched {len(contacts_for_update)} Contacts. Updating...')

    # Disable Process Builder processes
    sfdc_client.deactivate_pb_processes()

    bulk_job_id = sfdc_client.update_bulk(contacts_for_update)

    # If the bulk job isn't complete, wait 5 seconds then check again
    while not sfdc_client.check_bulk_job_complete(bulk_job_id):
        time.sleep(5)

    sfdc_client.activate_pb_processes()

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

    options, args = parser.parse_args()

    if options.headless_flag:
        run_sched()
    else:
        run_main()


