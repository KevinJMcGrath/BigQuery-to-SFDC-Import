import logging

import bq
import sfdc
from sfdc import payload


# 1. If a WSI_User_Detail__c entry already exists for a given username, update the values
# 2. If an entry does not exist, create a new one under the pool Id provided
# 3. If the Contact is not listed in the wsi_contacts list, lookup the contact
# 3a. Update Contact to be associated with the designated pool
# 3b. Insert WSI User Detail
# 4. Insert WSI User Detail History
# 5. Delete all WSI User Detail History records older than 30 days

def get_contact_by_username(username: str):
    soql = f"SELECT Id FROM Contact WHERE Username__c = '{username}'"

    results = sfdc.sfdc_client.soql_query(soql)

    if results:
        return results[0]
    else:
        return None

def get_wsi_user_details(active: bool = True):
    wsi_user_details = {}

    soql = 'SELECT Id, Contact__c, Contact_Username__c, Credits_Consumed__c, '
    soql += 'Pages_Consumed__c, WSI_Content_Pool__c '
    soql += ' FROM WSI_User_Detail__c'

    if active:
        soql += ' WHERE WSI_Content_Pool__r.Active__c = True'

    logging.info('Downloading WSI Pool User Details from Salesforce...')
    wsi_user_detail_records = sfdc.sfdc_client.soql_query(soql)

    for user_detail in wsi_user_detail_records:
        username = user_detail['Contact_Username__c'].lower()

        wsi_user_details[username] = user_detail

    return wsi_user_details

def get_contacts_by_wsi_pool():
    contact_map = {}

    soql = 'SELECT Id, Username__c, WSI_Content_Pool__c FROM Contact WHERE WSI_Content_Pool__c != NULL'

    logging.info('Downloading WSI-associated Contacts from Salesforce...')
    contacts = sfdc.sfdc_client.soql_query(soql)

    # Only obtain Contacts that have a Username
    for c in contacts:
        if 'Username__c' in c and c['Username__c']:
            uname = c['Username__c'].lower()
            contact_map[uname] = c

    return contact_map

# This method can create the history records for the User Details that were inserted below
def build_histories_new_records():
    histories = []

    soql = "SELECT Id, Contact_Username__c, Pages_Consumed__c, Credits_Consumed__c "
    soql += " FROM WSI_User_Detail__c WHERE CreatedDate = TODAY"

    logging.info('Downloading WSI records inserted today')
    wsi_user_detail_records = sfdc.sfdc_client.soql_query(soql)

    for user_detail in wsi_user_detail_records:
        wsi_ud_id = user_detail['Id']
        cred = user_detail['Credits_Consumed__c']
        pages = user_detail['Pages_Consumed__c']

        hist = {
            "WSI_User_Detail__c": wsi_ud_id,
            "Credits_Consumed__c": cred,
            "Pages_Consumed__c": pages
        }

        histories.append(hist)

    return histories


def update_wsi_consumption():
    query_str = f"SELECT * FROM {bq.bq_client.dataset_id}.{bq.bq_client.tables['wsi_consumption_table_id']}"
    bq.bq_client.query(query_str)

    wsi_user_details_insert = []
    wsi_user_details_update = []
    wsi_user_history_insert = []

    user_details = get_wsi_user_details()
    wsi_contacts = get_contacts_by_wsi_pool()

    for row in bq.bq_client.results:
        bq_pool_id = row['content_pool_id']
        bq_uname = row['username'].lower()
        uname_orig = row['username']

        if bq_uname in user_details:
            ud = user_details.get(bq_uname)

            p = payload.build_wsi_user_detail_update_payload(row, ud['Id'])
            wsi_user_details_update.append(p)

            p_h = payload.build_wsi_user_detail_history_payload(row, ud['Id'])
            wsi_user_history_insert.append(p_h)

        elif bq_uname in wsi_contacts:
            c = wsi_contacts.get(bq_uname)

            if not c or not c['Id']:
                logging.warning('Username not found in WSI Pool Contact list, skipping...')
                continue

            p = payload.build_wsi_user_detail_insert_payload(row, bq_pool_id, c['Id'])
            wsi_user_details_insert.append(p)
        else:
            logging.warning(f'Contact {bq_uname} found in BQ but not associated with a Content Pool in SFDC.')
            c = get_contact_by_username(uname_orig)

            if not c or not c['Id']:
                logging.warning('Username was not found in Salesforce, skipping...')
                continue

            p = payload.build_wsi_user_detail_insert_payload(row, bq_pool_id, c['Id'])
            wsi_user_details_insert.append(p)

    if len(wsi_user_details_update) > 0:
        logging.info('Updating existing WSI User Detail records...')
        sfdc.sfdc_client.update_bulk(wsi_user_details_update, 'WSI_User_Detail__c')

        sfdc.sfdc_client.monitor_job_queue()

        logging.info('Inserting WSI User Detail History records for updated User Detail records...')
        sfdc.sfdc_client.insert_bulk(wsi_user_history_insert, object_name='WSI_User_Detail_History__c')

        sfdc.sfdc_client.monitor_job_queue()

    if len(wsi_user_details_insert) > 0:
        logging.info('Inserting new WSI User Detail records...')
        job_id = sfdc.sfdc_client.insert_bulk(wsi_user_details_insert, 'WSI_User_Detail__c')

        sfdc.sfdc_client.monitor_job_queue()

        logging.info('Downloading Id\'s for newly created WSI User Detail records...')
        histories = build_histories_new_records()

        logging.info('Inserting WSI User Detail History records for newly created User Detail records...')
        sfdc.sfdc_client.insert_bulk(histories, object_name='WSI_User_Detail_History__c')

        sfdc.sfdc_client.monitor_job_queue()


