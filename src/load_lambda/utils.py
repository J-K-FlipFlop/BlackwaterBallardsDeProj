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
from numpy import nan


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


def get_latest_processed_file_list(
    client: boto3.client, timestamp_filtered: str = None
) -> dict:
    bucket = "blackwater-processed-zone"
    runtime_key = f"last_ran_at.csv"
    if not timestamp_filtered:
        get_previous_runtime = client.get_object(
            Bucket="blackwater-ingestion-zone", Key=runtime_key
        )
        timestamp = get_previous_runtime["Body"].read().decode("utf-8")
        timestamp_filtered = timestamp[12:-8]
    try:
        output = client.list_objects_v2(Bucket=bucket)
        if timestamp_filtered != "1999-12-31 23:59:59":
            file_list = [
                file["Key"]
                for file in output["Contents"]
                if timestamp_filtered in file["Key"]
            ]
        else:
            file_list = [
                file["Key"]
                for file in output["Contents"]
                if "original_data_dump" in file["Key"]
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


def get_insert_query(table_name: str, dataframe: pd.DataFrame):
    query = f"""INSERT INTO {table_name} """
    if table_name == "fact_sales_order":
        query += "(sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id) "
    if table_name == "fact_purchase_order":
        query += "(purchase_order_id, created_date, created_time, last_updated_date, last_updated_time, staff_id, counterparty_id, item_code, item_quantity, item_unti_price, currency_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id) "
    query += "VALUES "
    for _, row in dataframe.iterrows():
        query += f"""{tuple(row.values)}, """
    query = f"""{query[:-2]} RETURNING *;"""
    logger.info(query)
    query = query.replace("<NA>", "null")
    logger.info(query)
    return query


def insert_data_into_data_warehouse(client: boto3.client, pq_key: str, connection):
    data = get_data_from_processed_zone(client, pq_key)
    if data["status"] == "success":
        try:
            table_name = pq_key.split("/")[-1][:-8]
            table_name = sql_security(table_name)
            query = get_insert_query(table_name=table_name, dataframe=data["data"])
            connection.run(query)
            return {
                "status": "success",
                "table_name": table_name,
                "message": "Data successfully inserted into data warehouse",
            }
        except DatabaseError as e:
            return {
                "status": "failure",
                "table_name": table_name,
                "message": "Data was not added to data warehouse",
                "Error Message": e,
            }
        # finally:
        #     connection.close()
    else:
        return data


# dim_currency_dict = [{"currency_code": 'GBP',
#                      "currency_name": 'Pound Sterling'},
#                      {"currency_code": 'USD',
#                      "currency_name": 'US Dollar'},]

# df = pd.DataFrame(data=dim_currency_dict)
# query = "INSERT INTO dim_currency VALUES "
# for i, row in df.iterrows():
#     print(tuple(row.values))
#   row[j] for j in range(len(row)))
# print(df)
# df.to_parquet('currency.parquet', index=False)



# df = wr.s3.read_parquet(path=f"s3://blackwater-processed-zone/original_data_dump/dim_date.parquet")
# print(df)
# output_list = ''
# for _, row in df.iterrows():
#     output_list += f"{tuple(row.values)};"
# # print(df[df['design_name'].str.contains('"')])

# output = output_list.replace("<NA>", "null").replace("'s", "s").replace('"', "'")
# pp(output.split(';'))


