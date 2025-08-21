import time
import json
import logging

from validation import validate_source_file, unit_test_validation
from vault_utils import VaultClient
from config_model import AppConfig

def load_config_json(path: str) -> AppConfig:
    with open(path, 'r') as f:
        data = json.load(f)
    return AppConfig(**data)

def main():
    config = load_config_json("config.json")
    timestamp = time.strftime("%Y%m%d%H%M%S")

    validated_file = validate_source_file(config.source_table_list)

    if validated_file.empty:
        logging.warning("Source File Empty or invalid, Exiting The Process")
        return

    vault = VaultClient()
    unit_test_validation(
        project=config.project,
        subproject=config.subproject,
        db_src_id=config.source_conn_id,
        db_tgt_id=config.target_conn_id,
        tgt_path=config.target_directory,
        timestamp=timestamp,
        validated_df=validated_file,
        vault_client=vault
    )

if __name__ == "__main__":
    main()
