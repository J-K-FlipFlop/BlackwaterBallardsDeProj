import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound


# read from specific s3 bucket (ingestion-zone)
# find most recent folder
# read list of files in folder
# return dictionary containing the list
def read_latest_changes(client: boto3.client) -> dict:
    """Gets a list of most recently updated keys from S3 ingestion bucket

    Args:
        client: S3 Boto3 client

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            timestamp: the datetime that the most recent data was written to the ingestion zone
            file_list: a list of keys from the most recent folder in the ingestion zone
    """
    try:
        output = client.list_objects_v2(Bucket="blackwater-ingestion-zone")
        file_data = sorted(
            [k for k in output["Contents"]],
            key=lambda k: k["Key"],
            reverse=True,
        )
        runtime_key = "last_ran_at.csv"
        get_previous_runtime = client.get_object(Bucket='blackwater-ingestion-zone', Key=runtime_key)
        timestamp = get_previous_runtime["Body"].read().decode("utf-8")
        timestamp_filtered = timestamp.split("\n")[1]

        year = int(timestamp_filtered[0:4])

        if year < 2000:
            file_list = [
                file["Key"] for file in file_data if "original_data_dump" in file["Key"]
            ]
            timestamp_filtered ="original_data_dump"
        else:
            file_list = [
                file["Key"] for file in file_data if timestamp_filtered in file["Key"]
            ]

        return {
            "status": "success",
            "timestamp": timestamp_filtered,
            "file_list": file_list,
        }
    except ClientError as e:
        return {
            "status": "failure",
            "timestamp": "",
            "table_list": [],
            "message" : f"client error: returning empty table list {e.response}"
        }

def get_data_from_ingestion_bucket(
    key: str, filename: str, session: boto3.session.Session, update=True
) -> dict:
    """Downloads csv data from S3 ingestion bucket and returns a pandas dataframe

    Args:
        key: string representing S3 object to be downloaded
        session: Boto3 session

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            data: a pandas dataframe containing downloaded data (if successful)
            message: a relevant error message (if unsuccessful)
    """
    if update:
        path=f"s3://blackwater-ingestion-zone/ingested_data/{key}/{filename}"
    else:
        path=f"s3://blackwater-ingestion-zone/ingested_data/original_data_dump/{filename}"
    try:
        df = wr.s3.read_csv(
            path=path, boto3_session=session
        )
        # print(df.columns)
        return {"status": "success", "data": df}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    except NoFilesFound as nff:
        return {"status": "failure", "message": nff}


def write_parquet_data_to_s3(
    data: pd.DataFrame,
    table_name: str,
    session: boto3.session.Session,
    timestamp=None,
) -> dict:
    """Writes a pandas dataframe to S3 processed bucket in parquet format

    Args:
        data: a pandas dataframe
        table_name: name of table to be written
        session: Boto3 session
        timestamp: optional, passed into function so all files transformed by function are stored in same S3 folder

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            message: a relevant success/failure message
    """
    if isinstance(data, pd.DataFrame):
        try:
            wr.s3.to_parquet(
                df=data,
                path=f"s3://blackwater-processed-zone/{timestamp}/{table_name}.parquet",
                boto3_session=session,
            )
            return {
                "status": "success",
                "message": f"{table_name} written to processed bucket",
            }
        except ClientError as e:
            return {
                "status": "failure",
                "message": e.response,
            }
    else:
        return {
            "status": "failure",
            "message": f"Data is in wrong format {str(type(data))} is not a pandas dataframe",
        }

"""
{'columns': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 
'data': [[1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[2, 'Leannon, Predovic and Morar', 28, 'Melba Sanford', 'Jean Hane III', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[3, 'Armstrong Inc', 2, 'Jane Wiza', 'Myra Kovacek', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[4, 'Kohler Inc', 29, 'Taylor Haag', 'Alfredo Cassin II', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[5, 'Frami, Yundt and Macejkovic', 22, 'Homer Mitchell', 'Ivan Balistreri', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563']]}
"""
client = boto3.client("s3")
read_latest_changes(client)