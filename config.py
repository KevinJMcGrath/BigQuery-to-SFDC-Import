import jsonpickle

import models


with open('./config.json', 'r') as config_file:
    _config = jsonpickle.decode(config_file.read())

Google = models.GoogleSettings(_config['google'])
Salesforce = models.SalesforceSettings(_config['salesforce'])