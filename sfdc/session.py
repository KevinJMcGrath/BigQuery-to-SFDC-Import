import sfdc

from sfdc.client import SFClient


def load_client():
    if not sfdc.sfdc_client:
        sfdc.sfdc_client = SFClient.from_config()