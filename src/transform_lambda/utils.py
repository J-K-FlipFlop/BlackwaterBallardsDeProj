import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound


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


def get_data_from_ingestion_bucket(key, session):
    try:        
        df = wr.s3.read_csv(path=f's3://blackwater-ingestion-zone/{key}', boto3_session=session)
        print(df.columns)
        return {
            'status': 'success',
            'data': df
                }
    except ClientError as ce:
        return {
            'status': 'failure',
            'message': ce.response
        }
    except NoFilesFound as nff:
        return {
            'status': 'failure',
            'message': nff
        }

# loop through table list
# read data from s3 bucket for each file
# return data in some format


# transformation happens here


# convert to parquet
# write to s3 processed bucket