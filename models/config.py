from pathlib import Path

class GoogleSettings:
    def __init__(self, google_json):
        self.credentials = Path(google_json['sa_creds_path'])
        self.project_id = google_json['project_id']
        self.dataset_id = google_json['dataset_id']
        self.tables = google_json['tables']


class SalesforceSettings:
    def __init__(self, sfdc_json):
        self.username = sfdc_json['username']
        self.password = sfdc_json['password']
        self.security_token = sfdc_json['security_token']
        self.process_collection = {}