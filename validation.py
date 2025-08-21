import os
import pandas as pd
import logging
import traceback
from typing import List
from vault_utils import VaultClient
from db_utils import build_connection_string, get_db_engine, get_table_count, get_sample_records

def validate_source_file(file_path: str) -> pd.DataFrame:
    """
    Validate source file: check for duplicates and missing values.
    Logs issues to 'invalidfile.txt' in the source file directory.
    """
    logging.info(f"Validating source file: {file_path}")

    try:
        df = pd.read_csv(file_path, dtype=str)
    except Exception:
        logging.error(f"Error reading source list file:\n{traceback.format_exc()}")
        return pd.DataFrame()

    invalid_file_path = os.path.join(os.path.dirname(file_path), "invalidfile.txt")
    with open(invalid_file_path, 'w') as f:
        f.write("source_schema,source_table,column,target_schema,target_table,status\n")

    duplicates = df[df.duplicated()]
    duplicates["status"] = "duplicate record"

    invalids = df[df.isnull().any(axis=1)]
    invalids["status"] = "invalid record"

    for issue_df in [duplicates, invalids]:
        if not issue_df.empty:
            issue_df.to_csv(invalid_file_path, mode='a', header=False, index=False)

    if not duplicates.empty or not invalids.empty:
        logging.warning(f"Issues found and logged to {invalid_file_path}")

    return df.drop_duplicates().dropna()

def columns_match(src_df: pd.DataFrame, tgt_df: pd.DataFrame, columns: List[str]) -> bool:
    """Check if specified columns match between source and target dataframes."""
    try:
        if len(src_df) != len(tgt_df):
            logging.warning("Row count mismatch for sample records during column comparison.")
            return False

        src_subset = src_df[columns].reset_index(drop=True)
        tgt_subset = tgt_df[columns].reset_index(drop=True)

        return src_subset.equals(tgt_subset)
    except Exception:
        logging.error(f"Column comparison failed:\n{traceback.format_exc()}")
        return False

def unit_test_validation(
    project: str,
    subproject: str,
    db_src_id: str,
    db_tgt_id: str,
    tgt_path: str,
    timestamp: str,
    validated_df: pd.DataFrame,
    vault_client: VaultClient
):
    logging.info("Unit Testing Process Started")

    src_creds = vault_client.get_db_credentials(db_src_id)
    tgt_creds = vault_client.get_db_credentials(db_tgt_id)

    src_conn_str = build_connection_string(db_src_id, src_creds)
    tgt_conn_str = build_connection_string(db_tgt_id, tgt_creds)

    src_engine = get_db_engine(src_conn_str)
    tgt_engine = get_db_engine(tgt_conn_str)

    for _, row in validated_df.iterrows():
        src_table = f"{row['source_schema']}.{row['source_table']}"
        tgt_table = f"{row['target_schema']}.{row['target_table']}"
        columns = [col.strip() for col in row["column"].split(",")]

        src_count = get_table_count(src_engine, src_table)
        tgt_count = get_table_count(tgt_engine, tgt_table)

        if src_count == 0 or tgt_count == 0:
            logging.warning(f"No rows found or unable to fetch counts for {src_table} or {tgt_table}. Skipping.")
            continue

        if src_count != tgt_count:
            logging.warning(f"Row count mismatch: Source({src_count}) vs Target({tgt_count}) for {tgt_table}")
            continue

        src_sample = get_sample_records(src_engine, src_table)
        tgt_sample = get_sample_records(tgt_engine, tgt_table)

        if src_sample.empty or tgt_sample.empty:
            logging.warning(f"Unable to fetch sample data for {src_table} or {tgt_table}. Skipping.")
            continue

        if columns_match(src_sample, tgt_sample, columns):
            logging.info(f"Validation PASSED for {tgt_table}")
        else:
            logging.warning(f"Validation FAILED for {tgt_table}")

    src_engine.dispose()
    tgt_engine.dispose()
