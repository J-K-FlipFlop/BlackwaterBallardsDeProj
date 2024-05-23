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

def sql_security(table):
    conn = connect_to_db()
    table_names_unfiltered = conn.run(
        "SELECT TABLE_NAME FROM postgres.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
    )
    regex = re.compile("(^pg_)|(^sql_)|(^_)")
    table_names_filtered = [
        item[0] for item in table_names_unfiltered if not regex.search(item[0])
    ]
    if table in table_names_filtered:
        return table
    else:
        raise DatabaseError(
            "Table not found - do not start a table name with pg_, sql_ or _"
        )

def get_latest_processed_file_list(client: boto3.client, timestamp_filtered: str = None) -> dict:
    bucket = "blackwater-processed-zone"
    runtime_key = f"last_ran_at.csv"
    if not timestamp_filtered:
        get_previous_runtime = client.get_object(Bucket='blackwater-ingestion-zone', Key=runtime_key)
        timestamp = get_previous_runtime["Body"].read().decode("utf-8")
        timestamp_filtered = timestamp[12:-2]
    try:
        output = client.list_objects_v2(Bucket=bucket)
        file_list = [
            file["Key"]
            for file in output["Contents"]
            if timestamp_filtered in file["Key"]
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


def get_data_from_processed_zone(client: boto3.client, pq_key: str) -> dict:
    bucket = "blackwater-processed-zone"
    try:
        df = wr.s3.read_parquet(path=f"s3://{bucket}/{pq_key}")
        return {"status": "success", "data": df}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        return {"status": "failure", "message": c.response["Error"]["Message"]}


def insert_data_into_data_warehouse(
    client: boto3.client, pq_key: str
):
  data = get_data_from_processed_zone(client, pq_key)
  if data['status'] == 'success':
    try:
        table_name = pq_key.split('/')[-1][:-8]
        table_name = sql_security(table_name)
        conn = connect_to_db()
        query = f"INSERT INTO {table_name} VALUES "
        for i, row in data['data'].iterrows():
           query += f"({row['currency_code']}, {row['currency_name']}), "
        query = f"{query[:-2]};"
        # conn.run(query)
        return {"status": "success",
                "table_name": table_name,
                "message": "Data successfully inserted into data warehouse"}
    except DatabaseError as e:
        print(e)
        return {"status": "failure",
                "table_name": table_name,
                "message": "Data was not added to data warehouse"}
    # finally:
        # conn.close()
  else:
      return data




# df = pd.read_csv('test/data/TestGetFileContents.csv')
# pq_df = pd.read_parquet('test.parquet')
# print(pq_df)
# for i, row in df.iterrows():
#     print(f"{row['commercial_contact']}, {row['legal_address_id']}")
# df_dict = pq_df.to_dict('split')
# print(df_dict)
# query = 'INSERT INTO'
# for column in df_dict['columns']:
#     query += f" {column},"
# print(query)

# dim_currency_dict = [{"currency_code": 'GBP',
#                      "currency_name": 'Pound Sterling'},
#                      {"currency_code": 'USD',
#                      "currency_name": 'US Dollar'},]

# df = pd.DataFrame(data=dim_currency_dict)
# print(df)
# df.to_parquet('currency.parquet', index=False)