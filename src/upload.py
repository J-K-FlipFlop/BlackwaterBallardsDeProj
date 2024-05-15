from src.handler import connect_to_db_table, create_csv_data
import boto3
import logging
from botocore.exceptions import ClientError
import awswrangler as wr
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler():
    pass


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
    response =  wr.s3.to_csv(
        df=pd.DataFrame(data),
        path=f's3://{bucket}/{key}',
        boto3_session=session
        )
    print(response)
    return {"status": "success", "message": "written to bucket"}
