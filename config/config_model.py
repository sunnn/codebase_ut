from pydantic import BaseModel, FilePath, DirectoryPath
from typing import Optional

class AppConfig(BaseModel):
    project: str
    subproject: str
    source_conn_id: str
    target_conn_id: str
    credential_path: str
    target_directory: str
    source_table_list: str
