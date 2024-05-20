import boto3
from botocore.exceptions import ClientError
from datetime import datetime


# read from specific s3 bucket (ingestion-zone)
# find most recent folder
# read list of files in folder
# return dictionary containing the list
def read_latest_changes(client):
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
            "table_list": file_list,
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


# transformation happens here


# convert to parquet
# write to s3 processed bucket
