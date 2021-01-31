from datetime import datetime

def build_wsi_user_detail_update_payload(bq_row, ud_id: str):
    return {
        "Id": ud_id,
        "Credits_Consumed__c": bq_row['running_total_wsi_content_pool_credits_consumed'],
        "Pages_Consumed__c": bq_row['running_total_wsi_content_pool_page_purchases']
    }

def build_wsi_user_detail_insert_payload(bq_row, wsi_id: str=None, contact_id: str=None):
    return  {
        "Contact__c": contact_id,
        "WSI_Content_Pool__c": wsi_id,
        "Credits_Consumed__c": bq_row['running_total_wsi_content_pool_credits_consumed'],
        "Pages_Consumed__c": bq_row['running_total_wsi_content_pool_page_purchases']
    }

def build_wsi_user_detail_history_payload(bq_row, wsi_user_detail_id: str=None):
    return {
        "WSI_User_Detail__c": wsi_user_detail_id,
        "Credits_Consumed__c": bq_row['running_total_wsi_content_pool_credits_consumed'],
        "Pages_Consumed__c": bq_row['running_total_wsi_content_pool_page_purchases']
    }

def build_opp_payload(bq_row):
    opp_dict = {
        "Id": bq_row['opp_id'],
        "Health_Score_L3_Days_Logins__c": bq_row['sessions_health_score'],
        "Health_Score_Opp_Type__c": bq_row['opp_type_health_score'],
        "Health_Score_Overall__c": bq_row['opportunity_health_score'],
        "Health_Score_Percentage_Active_Trialers__c": bq_row['ttl_active_trialers_3d_perc'],
        "Health_Score_Percentage_of_Watchlists__c": bq_row['roll_dist_trialers_watchlists_created_buckets'],
        "Health_Score_Predicted_Stage__c": str(bq_row['predicted_stage_bucket']).title(),
        "Health_Score_Probability_to_Win__c": bq_row['prob'] * 10,
        "Health_Score_Sector__c": bq_row['sector_health_score'],
        "Health_Score_Watchlists__c": bq_row['watchlists_health_score']
    }

    return opp_dict

def build_renewal_payload(bq_row):
    opp_dict = {
        "Id": bq_row['opp_id'],
        "Renewal_Health_AS_Sector__c": bq_row['as_sector'],
        "Renewal_Health_AS_Sector_Rating__c": bq_row['as_sector_rating'],
        "Renewal_Health_ASV_Bucket__c": bq_row['asv_bucket'],
        "Renewal_Health_ASV_Bucket_Rating__c": bq_row['asv_bucket_rating'],
        "Renewal_Health_Month__c": bq_row['month'],
        "Renewal_Health_Months_Active_Days__c": bq_row['months_active_days_concat'],
        "Renewal_Health_Months_Active_Days_Rating__c": bq_row['months_active_days_rating'],
        "Renewal_Health_Months_to_Renewal__c": bq_row['months_to_renewal'],
        "Renewal_Health_Months_Watchlists__c": bq_row['months_watchlists'],
        "Renewal_Health_Months_Watchlists_Create__c": bq_row['months_watchlists_created_concat']
    }

    return opp_dict

def build_contact_payload(contact_id: str, bypass_toggle: bool, bq_row):
    def parse_datetime(row_dt):
        if row_dt:
            return row_dt.strftime("%Y-%m-%d")
        else:
            return ''

    contact_dict = {
        'Id': contact_id,
        'Apex_Bypass_Toggle__c': not bypass_toggle,
        'AS_BQ_Last_Updated__c': datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),

        'AS_Active_Days_7d__c': bq_row['active_days_in_last_7'],
        'AS_Active_Days_28d__c': bq_row['active_days_in_last_28'],
        'AS_Active_Days_90d__c': bq_row['active_days_in_last_90'],

        'AS_Documents_Read__c': bq_row['documents_read_count'],
        'AS_Documents_Read_7d__c': bq_row['documents_read_in_last_7'],
        'AS_Documents_Read_28d__c': bq_row['documents_read_in_last_28'],

        'AS_Watchlists_Created__c': bq_row['watchlists_created_count'],
        'AS_Watchlists_Deleted__c': bq_row['watchlists_deleted_count'],
        'AS_Watchlists_Modified__c': bq_row['watchlists_modified_count'],
        'AS_Watchlist_Searches_7d__c': bq_row['watchlist_searches_in_last_7'],
        'AS_Watchlist_Searches_28d__c': bq_row['watchlist_searches_in_last_28'],
        'AS_Net_Watchlists_Created__c': bq_row['net_watchlists_created'],

        'AS_Alerts_Created__c': bq_row['alert_creation_count'],
        'AS_Alerts_Deleted__c': bq_row['alert_deletion_count'],
        'AS_Active_Alerts_Subscribed__c': bq_row['active_alerts_subscribed'],
        'AS_Alerts_Subscribed_7d__c': bq_row['alerts_subscribed_in_last_7'],
        'AS_Alerts_Subscribed_28d__c': bq_row['alerts_subscribed_in_last_28'],

        'AS_Table_Export__c': bq_row['table_export_only_count'],
        'AS_Table_Export_Any_Event__c': bq_row['table_export_any_event_count'],

        'FactSet_AMR_Doc_Read_2d__c': bq_row['factset_amr_doc_read_today_yest'],
        'FactSet_AMR_Doc_Read_7d__c': bq_row['factset_amr_doc_read_in_last_7'],
        'FactSet_AMR_Doc_Read_30d__c': bq_row['factset_amr_doc_read_in_last_30'],
        'FactSet_AMR_Doc_Read_90d__c': bq_row['factset_amr_doc_read_in_last_90'],
        'FactSet_AMR_Doc_Read_365d__c': bq_row['factset_amr_doc_read_in_last_365'],
        'Factset_AMR_Doc_Purchase_2d__c': bq_row['factset_amr_doc_purchase_today_yest'],
        'Factset_AMR_Doc_Purchase_7d__c': bq_row['factset_amr_doc_purchase_in_last_7'],
        'Factset_AMR_Doc_Purchase_30d__c': bq_row['factset_amr_doc_purchase_in_last_30'],
        'Factset_AMR_Doc_Purchase_90d__c': bq_row['factset_amr_doc_purchase_in_last_90'],
        'Factset_AMR_Doc_Purchase_365d__c': bq_row['factset_amr_doc_purchase_in_last_365'],

        'FactSet_AMR_Pages_Read_2d__c': bq_row['factset_amr_page_read_today_yest'],
        'FactSet_AMR_Pages_Read_7d__c': bq_row['factset_amr_page_read_in_last_7'],
        'FactSet_AMR_Pages_Read_30d__c': bq_row['factset_amr_page_read_in_last_30'],
        'FactSet_AMR_Pages_Read_90d__c': bq_row['factset_amr_page_read_in_last_90'],
        'FactSet_AMR_Pages_Read_365d__c': bq_row['factset_amr_page_read_in_last_365'],
        'Factset_AMR_Page_Purchase_2d__c': bq_row['factset_amr_page_purchase_today_yest'],
        'Factset_AMR_Page_Purchase_7d__c': bq_row['factset_amr_page_purchase_in_last_7'],
        'Factset_AMR_Page_Purchase_30d__c': bq_row['factset_amr_page_purchase_in_last_30'],
        'Factset_AMR_Page_Purchase_90d__c': bq_row['factset_amr_page_purchase_in_last_90'],
        'Factset_AMR_Page_Purchase_365d__c': bq_row['factset_amr_page_purchase_in_last_365'],
        'FactSet_AMR_Pages_Consumed_2d__c': bq_row['factset_amr_pages_consumed_today_yest'],
        'FactSet_AMR_Pages_Consumed_7d__c': bq_row['factset_amr_pages_consumed_in_last_7'],
        'FactSet_AMR_Pages_Consumed_30d__c': bq_row['factset_amr_pages_consumed_in_last_30'],
        'FactSet_AMR_Pages_Consumed_90d__c': bq_row['factset_amr_pages_consumed_in_last_90'],
        'FactSet_AMR_Pages_Consumed_365d__c': bq_row['factset_amr_pages_consumed_in_last_365'],

        'Direct_Broker_Pages_Read_2d__c': bq_row['direct_broker_page_read_today_yest'],
        'Direct_Broker_Pages_Read_7d__c': bq_row['direct_broker_page_read_in_last_7'],
        'Direct_Broker_Pages_Read_30d__c': bq_row['direct_broker_page_read_in_last_30'],
        'Direct_Broker_Pages_Read_90d__c': bq_row['direct_broker_page_read_in_last_90'],
        'Direct_Broker_Pages_Read_365d__c': bq_row['direct_broker_page_read_in_last_365'],
        'Direct_Broker_Page_Purchase_2d__c': bq_row['direct_broker_page_purchase_today_yest'],
        'Direct_Broker_Page_Purchase_7d__c': bq_row['direct_broker_page_purchase_in_last_7'],
        'Direct_Broker_Page_Purchase_30d__c': bq_row['direct_broker_page_purchase_in_last_30'],
        'Direct_Broker_Page_Purchase_90d__c': bq_row['direct_broker_page_purchase_in_last_90'],
        'Direct_Broker_Page_Purchase_365d__c': bq_row['direct_broker_page_purchase_in_last_365'],
        'Direct_Broker_Pages_Consumed_2d__c': bq_row['direct_broker_pages_consumed_today_yest'],
        'Direct_Broker_Pages_Consumed_7d__c': bq_row['direct_broker_pages_consumed_in_last_7'],
        'Direct_Broker_Pages_Consumed_30d__c': bq_row['direct_broker_pages_consumed_in_last_30'],
        'Direct_Broker_Pages_Consumed_90d__c': bq_row['direct_broker_pages_consumed_in_last_90'],
        'Direct_Broker_Pages_Consumed_365d__c': bq_row['direct_broker_pages_consumed_in_last_365'],

        'AS_Total_iPhone_Sessions__c': bq_row['total_iphone_sessions'],
        'AS_iPhone_App_Sessions_7d__c': bq_row['iphone_app_sessions_in_last_7'],
        'AS_iPhone_App_Sessions_28d__c': bq_row['iphone_app_sessions_in_last_28']
    }

    dt_str = parse_datetime(bq_row['last_login_timestamp'])
    la_dt_str = parse_datetime(bq_row['last_activity_timestamp'])

    if dt_str:
        contact_dict['Last_Login__c'] = dt_str

    if la_dt_str:
        contact_dict['AS_Last_Activity__c'] = la_dt_str

    return contact_dict