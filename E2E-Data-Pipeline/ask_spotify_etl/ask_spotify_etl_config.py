"""application main configparser class"""

import configparser
import os


def singleton(cls, *args, **kw):
    """singleton pattern decorator function"""
    instances = {}

    def wrapper(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return wrapper


@singleton
class Config:
    """ configparser class.
        creates and stores the whole config in dict(config)"""

    config_file = os.path.join(os.path.dirname(__file__), 'config', 'ask_spotify_etl.conf')

    def __init__(self):
        config_parser = configparser.ConfigParser()
        config_parser.read(self.config_file)
        self.config = {}
        for section in config_parser.sections():
            self.config[section] = dict(config_parser.items(section))

    def get(self, section: str, key: str = None) -> str:
        """ method to return config value.
            section and key are to be provided as arguments."""

        config_section = self.config[section]

        if key:
            key_value = config_section[key]
            return key_value

        return config_section



if __name__ == '__main__':
    config = Config()
    print(config.get('general'))
