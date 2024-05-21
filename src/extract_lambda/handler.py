from src.extract_lambda.utils import (convert_table_to_dict,
                                       write_csv_to_s3,
                                       update_data_in_bucket)
import boto3
import os
import logging
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# pub_key = os.getenv("aws_access_key_id")
# priv_key = os.getenv("aws_secret_access_key")

session = boto3.session.Session()


def lambda_handler(event, context, session=None):
    runtime_key = f"last_ran_at.csv"
    bucket = "blackwater-ingestion-zone"
    table_list = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]

    time_of_day = datetime.now()

    for table in table_list:
        # data = convert_table_to_dict(table)
        key = f"playground/{table}.csv"
        response = update_data_in_bucket(table, bucket, session, time_of_day)
        if response["success"]:
            print(f"EXTRACTING TO: table: {table}, key: {key}")
        elif response['message'] == 'no new data':
            print('no new data to add ' + table)
        else:
            print(response["message"])
            return {"success": "false", "message": response["message"]}
    
    current_runtime = [{"last_ran_at": time_of_day}]
    write_csv_to_s3(
            session=session, data=current_runtime, bucket=bucket, key=runtime_key
        )
    return {"success": "true", "message": response["message"]}


# write_csv_to_s3()
# lambda_handler("yo", "jo", session)
