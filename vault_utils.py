import os
import hvac
import logging
import traceback
from typing import Dict

class VaultClient:
    """Vault client singleton wrapper to reuse connection."""

    def __init__(self):
        self.vault_addr = os.getenv("VAULT_ADDR", "http://127.0.0.1:8200")
        self.vault_token = os.getenv("VAULT_TOKEN", "dev-root")
        self.client = hvac.Client(url=self.vault_addr, token=self.vault_token)
        if not self.client.is_authenticated():
            raise ConnectionError("Vault authentication failed! Check VAULT_ADDR and VAULT_TOKEN.")

    def get_db_credentials(self, conn_id: str) -> Dict[str, str]:
        """Fetch DB credentials from Vault at secret/<conn_id>."""
        try:
            secret = self.client.secrets.kv.v2.read_secret_version(path=conn_id)
            return secret["data"]["data"]
        except Exception:
            logging.error(f"Failed to retrieve credentials for {conn_id}:\n{traceback.format_exc()}")
            raise
