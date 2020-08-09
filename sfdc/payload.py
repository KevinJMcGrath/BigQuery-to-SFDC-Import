from datetime import datetime

def build_payload(contact_id: str, bypass_toggle: bool, bq_row):
    def parse_datetime(row_dt):
        if row_dt:
            return row_dt.strftime("%Y-%m-%d")
        else:
            return ''

    contact_dict = {
        'Id': contact_id,
        'Username__c': bq_row['username'],
        'AS_Documents_Read__c': bq_row['documents_read_count'],
        'AS_Watchlists_Created__c': bq_row['watchlists_created_count'],
        'AS_Watchlists_Deleted__c': bq_row['watchlists_deleted_count'],
        'AS_Watchlists_Modified__c': bq_row['watchlists_modified_count'],
        'AS_Net_Watchlists_Created__c': bq_row['net_watchlists_created'],
        'AS_Alerts_Created__c': bq_row['alert_creation_count'],
        'AS_Alerts_Deleted__c': bq_row['alert_deletion_count'],
        'AS_BQ_Last_Updated__c': datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        'Apex_Bypass_Toggle__c': not bypass_toggle,
        'AS_Table_Export__c': bq_row['table_export_only_count'],
        'AS_Table_Export_Any_Event__c': bq_row['table_export_any_event_count'],
        'FactSet_AMR_Doc_Read_2d__c': bq_row['factset_amr_doc_read_today_yest'],
        'FactSet_AMR_Doc_Read_7d__c': bq_row['factset_amr_doc_read_in_last_7'],
        'FactSet_AMR_Doc_Read_30d__c': bq_row['factset_amr_doc_read_in_last_30'],
        'FactSet_AMR_Doc_Read_90d__c': bq_row['factset_amr_doc_read_in_last_90'],
        'FactSet_AMR_Doc_Read_365d__c': bq_row['factset_amr_doc_read_in_last_365'],
        'FactSet_AMR_Pages_Read_2d__c': bq_row['factset_amr_page_read_today_yest'],
        'FactSet_AMR_Pages_Read_7d__c': bq_row['factset_amr_page_read_in_last_7'],
        'FactSet_AMR_Pages_Read_30d__c': bq_row['factset_amr_page_read_in_last_30'],
        'FactSet_AMR_Pages_Read_90d__c': bq_row['factset_amr_page_read_in_last_90'],
        'FactSet_AMR_Pages_Read_365d__c': bq_row['factset_amr_page_read_in_last_365'],
        'Direct_Broker_Pages_Read_2d__c': bq_row['direct_broker_page_read_today_yest'],
        'Direct_Broker_Pages_Read_7d__c': bq_row['direct_broker_page_read_in_last_7'],
        'Direct_Broker_Pages_Read_30d__c': bq_row['direct_broker_page_read_in_last_30'],
        'Direct_Broker_Pages_Read_90d__c': bq_row['direct_broker_page_read_in_last_90'],
        'Direct_Broker_Pages_Read_365d__c': bq_row['direct_broker_page_read_in_last_365']

    }

    dt_str = parse_datetime(bq_row['last_login_timestamp'])
    la_dt_str = parse_datetime(bq_row['last_activity_timestamp'])

    if dt_str:
        contact_dict['Last_Login__c'] = dt_str

    if la_dt_str:
        contact_dict['AS_Last_Activity__c'] = la_dt_str

    return contact_dict