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

def get_api_token(credentials_file) -> str:
    token = read_yaml(credentials_file)["food_inspections"]["api_token"]
    return token
