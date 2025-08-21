import configparser
from typing import Dict

def parse_config(file_path: str) -> Dict[str, Dict[str, str]]:
    """Parse configuration file into nested dictionary."""
    config = configparser.ConfigParser()
    config.read(file_path)
    return {section: dict(config.items(section)) for section in config.sections()}
