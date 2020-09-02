import jsonpickle
import logging
import urllib.parse

import config


contact_pb_dict = {
    'AS_Premium_News_Content': 2,
    'Auto_Populate_Opportunity_Field_in_Contact_Record': 9,
    # 'Contact_Owner_equals_AM_when_Inactive_user_is_Activated': 3,
    'Corp_Investor_Relations_PositionId_Update': 2,
    'MCO_Financial_Inst_Global_AS_TRUE': 2,
    'MCO_Fundamental_Global_AS_TRUE': 2,
    'Stamp_MQL_Final_DateTime_Contacts': 2,
    'Set_up_trial_config': 9,
    'Transcript_Type_Default': 2,
    'Turn_of_direct_broker_feed': 2,
    'Turn_on_Moody_s_Credit_Research_Capital_Markets': 2,
    'Turn_on_underlying_MCO_Corporate_Global_AS_fields': 2
}

opp_pb_dict = {
    #'Delete_Content_of_Opportunity_field_in_Contact_if_Contact_Role_is_Deleted': None,
    'Assign_primary_contact': 3,
    'Opp_Stage_Date_Stamps': 3,
    'Update_Opportunity_Intacct_Entity': 1,
    'Deal_Source': 6,
    'Opportunity_Account_Manager_for_Won_Upsell_Opps': 4

}

class ProcessBuilderManager:
    def __init__(self, sfdc_client):
        self.client = sfdc_client

    def query_tooling_api(self, query):
        cleaned_query = urllib.parse.quote_plus(query)
        data = self.client.restful(path=f'tooling/query/?q={cleaned_query}')
        return data

    def get_all_pb_processes(self):
        query = 'Select Id,ActiveVersion.VersionNumber,LatestVersion.VersionNumber,DeveloperName From FlowDefinition'
        response = self.query_tooling_api(query)

        return {pb['Id']: pb for pb in response['records']}

    def submit_toggle_pb_process(self, process_id, version_num=None):
        pb = {
            'Metadata': {
                'activeVersionNumber': version_num
            }
        }

        pb_str = jsonpickle.encode(pb, unpicklable=False)
        response = None

        try:
            # The response coming from Salesforce is apparently malformed and fails to parse properly
            response = self.client.restful(path=f'tooling/sobjects/FlowDefinition/{process_id}/', method='PATCH', data=pb_str)
        except Exception as ex:
            # The tooling API always returns an error for this for some reason. Ignore it
            if 'Expecting value' not in str(ex):
                logging.error(ex)

    def toggle_processes(self, activate: bool, sobject: str):
        pb_map = self.get_all_pb_processes()

        for pb_id, pb in pb_map.items():
            pb_id = pb['Id']
            pb_name = pb['DeveloperName']

            working_dict = contact_pb_dict

            if sobject == 'Opportunity':
                working_dict = opp_pb_dict

            if pb_name in working_dict.keys():
                active_version = None

                if activate:
                    active_version = working_dict[pb_name]

                logging.info(f'{"Activating" if activate else "Deactivating"} process {pb_name}')
                self.submit_toggle_pb_process(pb_id, active_version)