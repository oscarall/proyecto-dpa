import pickle
import boto3

from datetime import date
from sodapy import Socrata

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
    guardar_ingesta(BUCKET, INGESTA_INICIAL_PATH, pickle.dumps(data))

def guardar_ingesta(bucket: str, bucket_path: str, data: bytes):
    s3 = get_s3_resource()
    ingestion_date = date.today().strftime("%Y-%m-%d")
    key = f"{bucket_path}-{ingestion_date}.pkl"
    response = s3.put_object(Body=data, Key=key, Bucket=bucket)
    print(response)

def ingesta_consecutiva(client: Socrata, date: str, limit=1000):
    soql_query = f"inspection_date >= '{date}'"
    data = client.get(DATASET_ID, limit=limit, where=soql_query)
    print(len(data))
    guardar_ingesta(BUCKET, INGESTA_CONSECUTIVA_PATH, pickle.dumps(data))

def get_s3_resource():
    s3_credentials = get_s3_credentials(CREDENTIALS_FILE)
    s3_resource = boto3.client("s3", **s3_credentials)
    return s3_resource
