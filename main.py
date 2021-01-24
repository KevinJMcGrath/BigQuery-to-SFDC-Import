import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from optparse import OptionParser

import package_logger

from sfdc import session
from sobjects import contact, opp, wsi

package_logger.initialize_logging()


def run_main():
    session.load_client()

    update_contacts()
    update_opps()
    update_wsi()


def update_wsi():
    wsi.update_wsi_consumption()


def update_opps():
    opp.update_opportunities()


def update_contacts(export_results: bool=False, record_count_limit: int=0):
    contact.update_contacts(export_results, record_count_limit)


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

    parser.add_option("-W", "--wsi", help="Manually execute import for WSI Consumption data.", dest="wsi_flag",
                      default=None, action="store_true")

    parser.add_option("-a", "--all", help="Manually execute import of all data immediately.", dest="all_flag",
                      default=None, action="store_true")

    options, args = parser.parse_args()

    if options.headless_flag:
        run_sched()
    elif options.opps_flag:
        session.load_client()
        update_opps()
    elif options.cnts_flag:
        session.load_client()
        update_contacts()
    elif options.wsi_flag:
        session.load_client()
        update_wsi()
    elif options.all_flag:
        run_main()
    else:
        run_main()