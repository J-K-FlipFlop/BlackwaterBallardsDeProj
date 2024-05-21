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
        latest_file_name = file_data[0]["Key"]
        timestamp = latest_file_name.split("/")[1]
        print(timestamp)
        file_list = [file["Key"] for file in file_data if timestamp in file["Key"]]
        return {
            "status": "success",
            "timestamp": timestamp,
            "file_list": file_list,
        }
    except ClientError:
        return {
            "status": "failure",
            "timestamp": "",
            "table_list": [],
        }


# loop through table list
# read data from s3 bucket for each file
# return data in some format


def get_data_from_ingestion_bucket(
    key: str, session: boto3.session.Session
) -> pd.DataFrame:
    try:
        df = wr.s3.read_csv(
            path=f"s3://blackwater-ingestion-zone/{key}", boto3_session=session
        )
        print(df.columns)
        return {"status": "success", "data": df}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    except NoFilesFound as nff:
        return {"status": "failure", "message": nff}


# transformation happens here


# convert to parquet
# write to s3 processed bucket
def write_parquet_data_to_s3(
    data: pd.DataFrame,
    table_name: str,
    session: boto3.session.Session,
    timestamp=None,
) -> dict:
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


# wr.s3.read_parquet creates a dataframe
# df.to_dict(orient='split', index=False) returns data in similar way to pg8000
# dict with columns and data as list of lists
"""
{'columns': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 
'data': [[1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[2, 'Leannon, Predovic and Morar', 28, 'Melba Sanford', 'Jean Hane III', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[3, 'Armstrong Inc', 2, 'Jane Wiza', 'Myra Kovacek', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[4, 'Kohler Inc', 29, 'Taylor Haag', 'Alfredo Cassin II', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'], 
[5, 'Frami, Yundt and Macejkovic', 22, 'Homer Mitchell', 'Ivan Balistreri', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563']]}
"""