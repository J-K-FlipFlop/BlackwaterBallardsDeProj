from src.extract_lambda.utils import (
    write_csv_to_s3,
    update_data_in_bucket,
)
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.session.Session(region_name="eu-west-2")


def lambda_handler(event, context, session: boto3.session = None) -> dict:
    """Lambda handler function to extract data from Totesys and write
    to S3 ingestion zone

    Args:
        Lambda function expects event and context, but are unused
        session: a Boto3 session (optional argument)

    Returns:
        Dictionary containing a sucess message and details of what has been
        written to ingestion bucket.
    """

    runtime_key = "last_ran_at.csv"
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

    try:
        get_previous_runtime = boto3.resource("s3").Object(bucket, runtime_key)
        previous_lambda_runtime_uncut = (
            get_previous_runtime.get()["Body"].read().decode("utf-8")
        )
        previous_lambda_runtime = datetime.strptime(
            previous_lambda_runtime_uncut[12:-2], "%Y-%m-%d %H:%M:%S.%f"
        )
        if str(previous_lambda_runtime)[:4] == "1999":
            previous_lambda_runtime = time_of_day
    except ClientError:
        previous_lambda_runtime = datetime(1999, 12, 31, 23, 59, 59, 99999)

    for table in table_list:
        key = f"{table}.csv"
        response = update_data_in_bucket(
            table, bucket, session, time_of_day, previous_lambda_runtime
        )
        if response["success"]:
            logger.info(f"Extracting to S3 bucket: table: {table}, key: {key}")
        elif response["message"] == "no new data":
            logger.info("no new data to add " + table)
        else:
            logger.info(response["message"])
            return {"success": "false", "message": response["message"]}

    current_runtime = [{"last_ran_at": time_of_day}]
    if previous_lambda_runtime < datetime(2000, 1, 1):
        current_runtime = [{"last_ran_at": previous_lambda_runtime}]
    write_csv_to_s3(
        session=session, data=current_runtime, bucket=bucket, key=runtime_key
    )
    message = {"success": "true", "message": response["message"]}
    logger.info(message)
    return message
