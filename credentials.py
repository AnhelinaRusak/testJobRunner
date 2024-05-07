"""Module that handles credentials.ini file"""
import configparser


def get_credentials(module_name: str):
    config = configparser.ConfigParser()
    config.read('credentials.ini')
    return config[module_name]
