class BQClient:
    def __init__(self, cred_path: str):
        self.cred_path = cred_path
        self.project_id = 'bia-prod-268616'
        self.dataset_id = 'salesforce_api'
        self.table_id = 'df_revops_user_activity_summary'