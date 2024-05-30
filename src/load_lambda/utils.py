from src.load_lambda.connection import connect_to_db
from botocore.exceptions import ClientError
from pg8000.exceptions import DatabaseError
import boto3
import logging
import pandas as pd
import re
import awswrangler as wr


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def sql_security(table: str) -> str:
    """Checks if the table passed exists in the Data Warehouse

    Args:
        table: table name as a string

    Returns:
        table: table name as a string, if it exists in the totesys database

    Raises:
        DatabaseError: if passed table name is not in the totesys database
    """

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
    """Gets a list of most recently updated keys from S3 processed bucket

    Args:
        client: S3 Boto3 client
        timestamp_filtered: datetime timestamp stored as a string

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            file_list: a list of keys from the most recent folder in the
            processed zone
    """

    bucket = "blackwater-processed-zone"
    runtime_key = "last_ran_at.csv"
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
    """Returns parquet data from S3 processed zone as a pandas dataframe

    Args:
        client: Boto3 client
        pq_key: the S3 object key name of the parquet file to be read

    Returns:
        A dictionary containing:
            A status message
            Data as a pandas dataframe (if successful)
            Error message (if unsuccessful)
    """
    bucket = "blackwater-processed-zone"
    try:
        df = wr.s3.read_parquet(path=f"s3://{bucket}/{pq_key}")
        return {"status": "success", "data": df}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        return {"status": "failure", "message": c.response["Error"]["Message"]}


def get_insert_query(table_name: str, dataframe: pd.DataFrame) -> str:
    """Returns a string containing a SQL INSERT statement

    Args:
        table_name: string containing a SQL table name
        dataframe: a correctly structured pandas dataframe containing data
        obtained from a parquet file

    Returns:
        A query string containing all values from dataframe as tuples
    """

    query = f"""INSERT INTO {table_name} """
    if table_name == "fact_sales_order":
        query += "(sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id) "
    query += "VALUES "
    for _, row in dataframe.iterrows():
        query += f"""{tuple(row.values)}, """
    query = f"""{query[:-2]} RETURNING *;"""
    query = query.replace("<NA>", "null")
    return query


def insert_data_into_data_warehouse(
    client: boto3.client, pq_key: str, connection
) -> dict:
    """Inserts data into data warehouse

    Args:
        client: Boto3 client
        pq_key: the S3 object key name of the parquet file to be read
        connection: pg8000 connection

    Returns:
        A dictionary containing the following:
            status message
            table name showing the table data was attempted to be inserted into
            error message (if unsuccessful)
    """

    data = get_data_from_processed_zone(client, pq_key)
    if data["status"] == "success":
        try:
            table_name = pq_key.split("/")[-1][:-8]
            table_name = sql_security(table_name)
            query = get_insert_query(
                table_name=table_name, dataframe=data["data"]
            )
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
    else:
        return data
