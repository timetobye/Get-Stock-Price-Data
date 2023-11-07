import sys
sys.path.append('/opt/airflow/')

from utils.configuration_control import ConfigurationControl


class ConfigurationUse(ConfigurationControl):
    def __init__(self):
        super().__init__()
        self.key, self.secret_key, self.token = self.get_config()

    def get_config(self):
        key, secret_key = self.get_application_keys()
        token = self.config_object.get('LIVE_APP', 'ACCESS_TOKEN')

        return key, secret_key, token
