import pickle
import boto3

from datetime import date
from sodapy import Socrata

from src.utils.general import get_api_token, get_s3_credentials

CREDENTIALS_FILE = "conf/local/credentials.yml"
DATASET_ID = "4ijn-s7e5"

def get_client() -> Socrata:
    token = get_api_token(CREDENTIALS_FILE)
    client = Socrata("data.cityofchicago.org", token)
    return client

def ingesta_inicial(client, limit) -> bytes:
    data = client.get(DATASET_ID, limit=limit)
    return pickle.dumps(data)

def guardar_ingesta(bucket, bucket_path, data):
    s3 = get_s3_resource()
    ingestion_date = date.today().strftime("%Y-%m-%d")
    key = f"{bucket_path}-{ingestion_date}.pkl"
    s3.put_object(Body=data, Key=key, Bucket=bucket)

def ingesta_consecutiva(client, date, limit=1000) -> bytes:
    soql_query = f"inspection_date >= '{date}'"
    data = client.get(DATASET_ID, limit=limit, where=soql_query)
    return pickle.dumps(data)

def get_s3_resource():
    s3_credentials = get_s3_credentials(CREDENTIALS_FILE)
    s3_resource = boto3.client("s3", **s3_credentials)
    return s3_resource
