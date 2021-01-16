import sfdc

def disable_process_builder(object_name: str):
    sfdc.sfdc_client.deactivate_pb_processes(object_name)

def enable_process_builder(object_name: str):
    sfdc.sfdc_client.activate_pb_processes(object_name)