import pandas as pd
from pg8000.exceptions import DatabaseError
from pg8000.native import Connection
import boto3
import logging
from botocore.exceptions import ClientError
import awswrangler as wr
import ast

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
session = boto3.session.Session()

def get_secret():

    secret_name = "totesys-blackwater-credentials"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]
    # print(secret["username"])
    return ast.literal_eval(secret)

def connect_to_db():
    creds = get_secret()

    user = creds["username"]
    password = creds["password"]
    database = creds["dbname"]
    host = creds["host"]
    port = creds["port"]
    return Connection(
        user=user, password=password, database=database, port=port, host=host
    )

def connect_to_db_table(table):
    try:
        conn = connect_to_db()
        result = conn.run(f"Select * From {table};")
        columns = [col["name"] for col in conn.columns]
        sales_order_dicts = [dict(zip(columns, a)) for a in result]
        return sales_order_dicts
    except DatabaseError:
        error_message = f'relation "{table}" does not exist'
        return {"status": "Failed", "message": error_message}
    
def write_csv_to_s3(session, data, bucket, key):
    try:
        response =  wr.s3.to_csv(
            df=pd.DataFrame(data),
            path=f's3://{bucket}/{key}',
            boto3_session=session,
            index=False
            )
        return {"status": "success", "message": "written to bucket"}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        return {"status": "failed", "message": c.response["Error"]["Message"]}
    
def lambda_handler(event, context):
    bucket = "blackwater-ingestion-zone"
    table_list = ["counterparty", "currency", "department", "design", "staff", 
                  "sales_order", "address", "payment", "purchase_order",
                  "payment_type", "transaction"]

    for table in table_list:
        data = connect_to_db_table(table)
        key = f"playground/{table}.csv"
        print(f"EXTRACTING TO: table: {table}, key: {key}")
        try:
            write_csv_to_s3(session, data, bucket, key)
        except:
            return {"success": "false"}
    return {"success": "true"}
