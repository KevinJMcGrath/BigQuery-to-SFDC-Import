import jsonpickle

import models.config


with open('./config.json', 'r') as config_file:
    _config = jsonpickle.decode(config_file.read())

LogVerbose = _config['log_verbose']

Google = models.config.GoogleSettings(_config['google'])
Salesforce = models.config.SalesforceSettings(_config['salesforce'])

for pb_item in _config['salesforce']['pb_collection']:
    Salesforce.process_collection[pb_item['name']] = pb_item['version']