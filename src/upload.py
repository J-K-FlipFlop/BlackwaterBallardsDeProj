from src.handler import connect_to_db_table, create_csv_data
import boto3
import os
import logging
from botocore.exceptions import ClientError
import awswrangler as wr
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# pub_key = os.getenv("aws_access_key_id")
# priv_key = os.getenv("aws_secret_access_key")

# session = boto3.session.Session()

def write_to_s3(client, data, bucket, key):
    """Helper to write material to S3."""
    body = data
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body)
        return {"status": "success", "message": "written to bucket"}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        return {"status": "failed", "message": c.response["Error"]["Message"]}
    
def write_csv_to_s3(session, data, bucket, key):
    try:
        response =  wr.s3.to_csv(
            df=pd.DataFrame(data),
            path=f's3://{bucket}/{key}',
            boto3_session=session,
            index=False
            )
        return {"success": True, "message": "written to bucket"}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        response = {"success": False, "message": c.response["Error"]["Message"]}
        print(response)
        return response
    

    
def lambda_handler(event, context, session):
    bucket = "bucket-for-my-emotions"
    table_list = ["counterparty", "currency", "department", "wrong_name", "staff", 
                  "sales_order", "address", "payment", "purchase_order",
                  "payment_type", "transaction"]

    for table in table_list:
        data = connect_to_db_table(table)
        key = f"playground/{table}.csv"
        response = write_csv_to_s3(session, data, bucket, key)
        if response["success"]:
            print(f"EXTRACTING TO: table: {table}, key: {key}")
        else:
            return {"success": "false"}
    return {"success": "true"}

# write_csv_to_s3()
# lambda_handler()