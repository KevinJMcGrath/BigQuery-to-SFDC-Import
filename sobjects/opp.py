import logging

import bq
import sfdc

from sfdc import payload
from sobjects import pb

def update_opportunities():
    pb.disable_process_builder(object_name='Opportunity')

    update_opps_health_score()

    # Disabled intentionally, called "whacky bullshit" for some reason
    # update_opps_renewal()

    pb.enable_process_builder(object_name='Opportunity')

def update_opps_health_score():
    query_str = f"SELECT * FROM {bq.bq_client.dataset_id}.{bq.bq_client.tables['opp_table_id']}"
    bq.bq_client.query(query_str)

    # They're giving me the opp Id. I don't really need to match them.
    # opp_ids = sfdc_client.get_open_opps()

    unique_opps = {}
    opps_for_update = []

    for row in bq.bq_client.results:
        o_id = row['opp_id']
        row_num = row['row_num']

        if o_id in unique_opps:
            if row_num > unique_opps[o_id][0]:
                unique_opps[o_id] = (row_num, payload.build_opp_payload(row))
        else:
            unique_opps[o_id] = (row_num, payload.build_opp_payload(row))

    for k, v in unique_opps.items():
        opp_payload = v[1]
        opps_for_update.append(opp_payload)

    sfdc.sfdc_client.update_bulk(opps_for_update, object_name='Opportunity')

    sfdc.sfdc_client.monitor_job_queue()

    logging.info('done!')

def update_opps_renewal():
    query_str = f"SELECT * FROM {bq.bq_client.dataset_id}.{bq.bq_client.tables['opp_renewal_table_id']}"
    bq.bq_client.query(query_str)

    opps_for_update = []

    for row in bq.bq_client.results:
        opps_for_update.append(payload.build_renewal_payload(row))

    sfdc.sfdc_client.update_bulk(opps_for_update, object_name='Opportunity')

    sfdc.sfdc_client.monitor_job_queue()

    logging.info('done!')