import sys
import configparser
import os
import time
import pandas as pd
import re
import logging
from sqlalchemy import create_engine
import hvac
from typing import Dict, Tuple

# ------------------ Setup Logging ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


# ------------------ Config Parser ------------------
def cfgParse(cfgParam: str) -> Dict[str, Dict[str, str]]:
    """Parses configuration file into a nested dictionary."""
    cfgParser = configparser.ConfigParser()
    cfgParser.read(cfgParam)
    dictionary = {}
    for section in cfgParser.sections():
        dictionary[section] = {opt: cfgParser.get(section, opt) for opt in cfgParser.options(section)}
    return dictionary


# ------------------ Vault Utilities ------------------
def vault_client():
    """Initialize Vault client using environment variables."""
    vault_addr = os.getenv("VAULT_ADDR", "http://127.0.0.1:8200")
    vault_token = os.getenv("VAULT_TOKEN", "dev-root")

    client = hvac.Client(url=vault_addr, token=vault_token)
    if not client.is_authenticated():
        raise Exception("Vault authentication failed! Check VAULT_ADDR and VAULT_TOKEN.")
    return client


def dbutils_from_vault(conn_id: str) -> Tuple[str, str, str, str, str]:
    """
    Retrieve DB credentials from Vault.
    Expects secrets stored at secret/<conn_id>.
    """
    client = vault_client()
    try:
        secret = client.secrets.kv.v2.read_secret_version(path=conn_id)
        creds = secret["data"]["data"]

        servername = creds["servername"]
        database = creds["database"]
        port = creds["port"]
        username = creds["username"]
        password = creds["password"]

        return servername, database, port, username, password
    except Exception as e:
        raise Exception(f"Could not fetch credentials from Vault for {conn_id}: {e}")


def connection(conn_id: str) -> str:
    """Build SQLAlchemy connection string from Vault secrets."""
    servername, database, port, username, password = dbutils_from_vault(conn_id)

    if conn_id.endswith("_mysql"):
        return f"mysql+pymysql://{username}:{password}@{servername}/{database}"
    elif conn_id.endswith("_gpsql"):
        return f"postgresql+psycopg2://{username}:{password}@{servername}:{port}/{database}"
    else:
        raise ValueError(f"Unsupported connection type: {conn_id}")


# ------------------ Source File Validation ------------------
def SourceFileCheck(source_list: str) -> pd.DataFrame:
    """Validates source table list file (checks duplicates & missing values)."""
    logging.info("Reading source file: %s", source_list)

    try:
        frdr = pd.read_csv(source_list, header='infer', dtype=str)
    except Exception as e:
        logging.error("Error reading source list file: %s", e)
        return pd.DataFrame()

    path = os.path.dirname(source_list)
    InvalidFile = os.path.join(path, "invalidfile.txt")

    with open(InvalidFile, 'w') as wrtr:
        wrtr.write("source_schema,source_table,column,target_schema,target_table,status\n")

    # Duplicate check
    dup = frdr[frdr.duplicated()].copy()
    dup["status"] = "duplicate record"

    # Null value check
    clm = frdr[frdr.isnull().any(axis=1)].copy()
    clm["status"] = "invalid record"

    # Save issues
    for df in [dup, clm]:
        if not df.empty:
            df.to_csv(InvalidFile, sep=',', header=False, mode='a', index=False)

    # Clean valid records
    frdr.drop_duplicates(inplace=True)
    frdr.dropna(inplace=True)

    return frdr


# ------------------ DB Queries ------------------
def tbl_count(conn_id: str, tbl_hldr: str) -> pd.DataFrame:
    """Returns row count of a given table."""
    query = f"SELECT '{tbl_hldr.split('.')[-1]}' AS table_name, COUNT(1) AS count FROM {tbl_hldr}"
    db_connection_str = connection(conn_id)
    dbconn = create_engine(db_connection_str)

    try:
        return pd.read_sql(query, dbconn)
    except Exception as e:
        logging.error("Count query failed for %s: %s", tbl_hldr, e)
        return pd.DataFrame()
    finally:
        dbconn.dispose()


def tblrcrd(conn_id: str, hldr: str) -> pd.DataFrame:
    """Fetches sample records from a table."""
    query = f"SELECT * FROM {hldr} LIMIT 10"
    db_connection_str = connection(conn_id)
    dbconn = create_engine(db_connection_str)

    try:
        return pd.read_sql(query, dbconn)
    except Exception as e:
        logging.error("Error fetching records from %s: %s", hldr, e)
        return pd.DataFrame()
    finally:
        dbconn.dispose()


# ------------------ Validation Logic ------------------
def pcol_check(src_pcol: pd.DataFrame, tgt_pcol: pd.DataFrame, column) -> bool:
    """Checks if column(s) match between source and target samples."""
    if isinstance(column, str):
        column = [column]

    try:
        result = (src_pcol[column].reset_index(drop=True) == tgt_pcol[column].reset_index(drop=True))
        return result.all().all()
    except Exception as e:
        logging.error("Primary column check failed: %s", e)
        return False


def unit_test_validation(project, subproject, db_src_id, db_tgt_id, tgt_path, timestamp, ValidatedFile):
    logging.info("Unit Testing Process Started")
    for _, row in ValidatedFile.iterrows():
        SrcHldr = f"{row['source_schema']}.{row['source_table']}"
        TgtHldr = f"{row['target_schema']}.{row['target_table']}"

        srcnt = tbl_count(db_src_id, SrcHldr)
        tgtnt = tbl_count(db_tgt_id, TgtHldr)

        if not srcnt.empty and not tgtnt.empty:
            if srcnt["count"].iloc[0] == tgtnt["count"].iloc[0]:
                column = row["column"].split(",") if "," in row["column"] else row["column"]
                src_pcol = tblrcrd(db_src_id, SrcHldr)
                tgt_pcol = tblrcrd(db_tgt_id, TgtHldr)

                if not src_pcol.empty and not tgt_pcol.empty:
                    match = pcol_check(src_pcol, tgt_pcol, column)
                    logging.info("Validation result for %s: %s", TgtHldr, "PASS" if match else "FAIL")
            else:
                logging.warning("Count mismatch: %s vs %s for %s", srcnt, tgtnt, TgtHldr)
        else:
            logging.warning("Could not fetch counts for: %s", TgtHldr)


# ------------------ Main ------------------
if __name__ == '__main__':
    cfgParam = 'unit_test_config.txt'
    cfgBuilder = cfgParse(cfgParam)

    default_cfg = cfgBuilder.get("default", {})
    project = default_cfg.get("project")
    subproject = default_cfg.get("subproject")
    db_src_id = default_cfg.get("source_conn_id")
    db_tgt_id = default_cfg.get("target_conn_id")
    tgt_path = default_cfg.get("target_directory")
    source_list = default_cfg.get("source_table_list")
    timestamp = time.strftime("%Y%m%d%H%M%S")

    ValidatedFile = SourceFileCheck(source_list)

    if not ValidatedFile.empty:
        unit_test_validation(project, subproject, db_src_id, db_tgt_id, tgt_path, timestamp, ValidatedFile)
    else:
        logging.warning("Source File Empty, Exiting The Process")
