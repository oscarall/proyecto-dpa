import json

from luigi.contrib.postgres import CopyToTable
from src.utils.general import get_db_credentials
from src.utils.constants import CREDENTIALS_FILE

class DPACopyToTable(CopyToTable):
    credentials = get_db_credentials(CREDENTIALS_FILE)

    user = credentials['user']
    password = credentials['pass']
    database = credentials['database']
    host = credentials['host']
    port = credentials['port']
    table = 'dpa.task_metadata'

    columns = ["step", "task_id","metadata"]

    def rows(self):
        with self.requires().input()[1].open("r") as input_target:
            data = input_target.read()
        
        metadata = json.loads(data)

        yield (metadata["step"], metadata["id"], json.dumps(metadata["metadata"]))