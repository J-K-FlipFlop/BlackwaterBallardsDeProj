from src.extract_lambda.utils import convert_table_to_dict, write_csv_to_s3
import boto3
import os
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# pub_key = os.getenv("aws_access_key_id")
# priv_key = os.getenv("aws_secret_access_key")

session = boto3.session.Session()


def lambda_handler(event, context, session=None):
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

    for table in table_list:
        data = convert_table_to_dict(table)
        key = f"playground/{table}.csv"
        response = write_csv_to_s3(session, data, bucket, key)
        if response["success"]:
            print(f"EXTRACTING TO: table: {table}, key: {key}")
        else:
            print(response["message"])
            return {"success": "false", "message": response["message"]}
    return {"success": "true", "message": response["message"]}


# write_csv_to_s3()
# lambda_handler("yo", "jo", session)
