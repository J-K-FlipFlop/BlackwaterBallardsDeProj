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

def get_data_from_processed_zone(client: boto3.session.Session, pq_table_name: str):
    bucket = "blackwater-processed-zone"
    filepath_to_parquet = f'{pq_table_name}.parquet'
    try:
        parquet_data_table = boto3.resource("s3").Object(bucket, filepath_to_parquet)
        extracted_parquet_result = parquet_data_table.get()
        return extracted_parquet_result
    except ClientError as c:
        logger.info(f'Boto3 ClientError: {str(c)}')
        return {"success": False, "message": c.response["Error"]["Message"]}
