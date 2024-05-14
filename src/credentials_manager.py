import boto3
from botocore.exceptions import ClientError
import ast
import os
from dotenv import load_dotenv

load_dotenv()

pub_key = os.getenv('aws_access_key_id')
priv_key = os.getenv('aws_secret_access_key')


def get_secret():

    secret_name = "totesys-blackwater-credentials"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session(aws_access_key_id=pub_key, aws_secret_access_key=priv_key)
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    # print(secret["username"])
    return ast.literal_eval(secret)
