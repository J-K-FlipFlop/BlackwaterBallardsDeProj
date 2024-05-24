from src.extract_lambda.connection import connect_to_db
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


def convert_table_to_dict(table):
    table = sql_security(table)
    try:
        conn = connect_to_db()
        result = conn.run(f"Select * From {table};")
        columns = [col["name"] for col in conn.columns]
        sales_order_dicts = [dict(zip(columns, a)) for a in result]
        return sales_order_dicts
    except DatabaseError:
        error_message = f'relation "{table}" does not exist'
        return {"status": "Failed", "message": error_message}
    finally:
        conn.close()


def sql_security(table):
    conn = connect_to_db()
    table_names_unfiltered = conn.run(
        "SELECT TABLE_NAME FROM totesys.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
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
        response = wr.s3.to_csv(
            df=pd.DataFrame(data),
            path=f"s3://{bucket}/{key}",
            boto3_session=session,
            index=False,
        )
        return {"success": True, "message": "written to bucket"}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        response = {"success": False, "message": c.response["Error"]["Message"]}
        # print(response)
        return response


def update_data_in_bucket(table: str, bucket, session, time_of_day):
    table_info = convert_table_to_dict(table)
    runtime_key = f"last_ran_at.csv"
    try:
        get_previous_runtime = boto3.resource("s3").Object(bucket, runtime_key)
        previous_lambda_runtime_uncut = (
            get_previous_runtime.get()["Body"].read().decode("utf-8")
        )
        # if structure of blackwater-ingestion-zone/last_ran_at changes slice on line below will likely have to be updated too
        previous_lambda_runtime = datetime.strptime(
            previous_lambda_runtime_uncut[12:-2], "%Y-%m-%d %H:%M:%S.%f"
        )
    except:
        previous_lambda_runtime = datetime(1999, 12, 31, 23, 59, 59, 999999)
    pp(previous_lambda_runtime)
    new_items = []
    # current_lambda_runtime = datetime.now()

    for item in table_info:
        item_latest_update = item["last_updated"]
        if item_latest_update > previous_lambda_runtime:
            new_items.append(item)

    data = new_items
    if previous_lambda_runtime < datetime(2000, 1, 1, 1, 1):
        key = f"ingested_data/original_data_dump/{table}.csv"
    else:
        key = f"ingested_data/{time_of_day}/{table}.csv"
    if new_items:
        response = write_csv_to_s3(session=session, data=data, bucket=bucket, key=key)
    else:
        response = {"success": False, "message": "no new data"}

    return response
