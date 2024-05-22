import boto3
from botocore.exceptions import ClientError
import ast
import os

###
### Uncomment below code and code in function to connect on local machine
###
# from dotenv import load_dotenv

# load_dotenv()

# pub_key = os.getenv("aws_access_key_id")
# priv_key = os.getenv("aws_secret_access_key")


def get_secret():

    secret_name = "warehouse-blackwater-credentials"
    region_name = "eu-west-2"

    session = boto3.session.Session(
        # uncomment out below code to run on local machine
        # aws_access_key_id=pub_key,
        # aws_secret_access_key=priv_key,
    )
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return ast.literal_eval(secret)
