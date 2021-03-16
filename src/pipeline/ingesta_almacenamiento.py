import pickle
import boto3

from datetime import date
from sodapy import Socrata
import luigi.contrib.s3

from src.utils.general import get_api_token, get_s3_credentials
from src.utils.constants import (
    CREDENTIALS_FILE,
    DATASET_ID,
    BUCKET,
    INGESTA_CONSECUTIVA_PATH,
    INGESTA_INICIAL_PATH
)

def get_client() -> Socrata:
    token = get_api_token(CREDENTIALS_FILE)
    client = Socrata("data.cityofchicago.org", token)
    return client

def ingesta_inicial(client, limit: int):
    data = client.get(DATASET_ID, limit=limit)
    return pickle.dumps(data)

def ingesta_consecutiva(client: Socrata, date: str, limit=1000):
    soql_query = f"inspection_date >= '{date}'"
    data = client.get(DATASET_ID, limit=limit, where=soql_query)
    return pickle.dumps(data)

def get_s3_resource():
    s3_credentials = get_s3_credentials(CREDENTIALS_FILE)
    return luigi.contrib.s3.S3Client(**s3_credentials)
