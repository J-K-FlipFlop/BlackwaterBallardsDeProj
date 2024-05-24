from src.extract_lambda.utils import (
    write_csv_to_s3,
    update_data_in_bucket,
)
import boto3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.session.Session()


def lambda_handler(event, context, session=None):
    """Lambda handler function to extract data from Totesys and write to S3 ingestion zone"""

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
            logger.info(f"Extracting to S3 bucket: table: {table}, key: {key}")
        elif response["message"] == "no new data":
            logger.info("no new data to add " + table)
        else:
            logger.info(response["message"])
            return {"success": "false", "message": response["message"]}

    current_runtime = [{"last_ran_at": time_of_day}]
    write_csv_to_s3(
        session=session, data=current_runtime, bucket=bucket, key=runtime_key
    )
    message = {"success": "true", "message": response["message"]}
    logger.info(message)
    return message


# write_csv_to_s3()
# lambda_handler("yo", "jo", session)
