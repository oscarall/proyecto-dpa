import yaml

def read_yaml(credentials_file) -> dict:
    config = None
    try: 
        with open (credentials_file, 'r') as f:
            config = yaml.safe_load(f)
    except:
        raise FileNotFoundError("Couldnt load the file")
    
    return config

def get_s3_credentials(credentials_file) -> dict:
    s3_credentials = read_yaml(credentials_file)["s3"]
    return s3_credentials

def get_db_credentials(credentials_file) -> dict:
    db_credentials = read_yaml(credentials_file)["db"]
    return db_credentials

def get_api_token(credentials_file) -> str:
    token = read_yaml(credentials_file)["food_inspections"]["api_token"]
    return token

def get_db_conn_sql_alchemy(credentials_file) -> dict:
    alchemy_credentials = read_yaml(credentials_file)["db"]
    connection = "postgresql://{}:{}@{}:{}/{}".format(alchemy_credentials['user'], alchemy_credentials['pass'], alchemy_credentials['host'], alchemy_credentials['port'], alchemy_credentials['database']) 
    return connection
