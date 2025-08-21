import time
import logging
from config_utils import parse_config
from validation import validate_source_file, unit_test_validation
from vault_utils import VaultClient

def main():
    cfg_file = 'unit_test_config.txt'
    cfg = parse_config(cfg_file)

    default_cfg = cfg.get("default", {})
    project = default_cfg.get("project")
    subproject = default_cfg.get("subproject")
    db_src_id = default_cfg.get("source_conn_id")
    db_tgt_id = default_cfg.get("target_conn_id")
    tgt_path = default_cfg.get("target_directory")
    source_list = default_cfg.get("source_table_list")
    timestamp = time.strftime("%Y%m%d%H%M%S")

    validated_file = validate_source_file(source_list)

    if validated_file.empty:
        logging.warning("Source File Empty or invalid, Exiting The Process")
        return

    vault = VaultClient()
    unit_test_validation(project, subproject, db_src_id, db_tgt_id, tgt_path, timestamp, validated_file, vault)

if __name__ == "__main__":
    main()
