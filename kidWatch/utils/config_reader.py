import os

import yaml


class ConfigReader:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ConfigReader, cls).__new__(cls, *args, **kwargs)
            cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self):
        with open(os.getcwd() + '/config/config.yaml', 'r', encoding='utf-8') as file:
            self.config = yaml.safe_load(file)
        self.is_internal = self.get_config('is_internal')

    def get_config(self, key=None):
        return self.config.get(key, {}) if key else self.config

    def get_smb_config(self):
        return self.get_config('smb_internal') if self.is_internal else self.get_config('smb')
