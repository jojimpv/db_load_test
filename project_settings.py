import os

from st_connectors.key_vault.keyvault_secrets import SettingsConnector

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

settings = SettingsConnector(dynaconf_file=f"{ROOT_DIR}/settings.toml")


def get_root_path():
    return ROOT_DIR
