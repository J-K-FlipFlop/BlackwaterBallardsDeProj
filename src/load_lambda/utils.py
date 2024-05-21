from src.load_lambda.connection import connect_to_db
from botocore.exceptions import ClientError
from pg8000.exceptions import DatabaseError
from datetime import datetime
from pprint import pprint as pp
import boto3
import logging
import pandas as pd
import re
import awswrangler as wr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""Pseudocode
get data from s3 function
    all data in processed bucket in parquet format
    pass table_name.parquet as argument to get data from bucket,
    using get_object, one by one. Takes table_name, which references
    filepath of the data in the processed zone

insert data into warehouse
    want to load data one by one, so get parquet data and 
    immediately INSERT INTO warehouse, using table_name as the 
    name of the table. can INSERT directly, using read_parquet
    functionality
"""


def get_latest_processed_file_list(client: boto3.client) -> dict:
    bucket = "blackwater-processed-zone"
    timestamp = "2024-05-20 12:10:03.998128"
    try:
        output = client.list_objects_v2(Bucket=bucket)
        file_list = [
            file["Key"]
            for file in output["Contents"]
            if timestamp in file["Key"]
        ]
        return {
            "status": "success",
            "file_list": file_list,
        }
    except ClientError:
        return {
            "status": "failure",
            "file_list": [],
        }


def get_data_from_processed_zone(
    client: boto3.session.Session, pq_key: str
) -> dict:
    bucket = "blackwater-processed-zone"
    try:
        df = wr.s3.read_parquet(path=f"s3://{bucket}/{pq_key}")
        return {"status": "success", "data": df}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        return {"status": "failure", "message": c.response["Error"]["Message"]}
