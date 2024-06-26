from src.load_lambda.utils import (
    get_latest_processed_file_list,
    insert_data_into_data_warehouse,
    connect_to_db,
)
import boto3
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.session.Session(region_name="eu-west-2")


def load_lambda_handler(event, context) -> dict:
    """Lambda handler function to read data from the S3 processed zone and
    load data to the Data Warehouse

    Args:
        Lambda function expects event and context, but are unused within the
        function

    Returns:
        Dictionary containing a sucess message and details of what has been
        written to ingestion bucket.
    """

    conn = connect_to_db()
    client = session.client("s3")
    processed_files = get_latest_processed_file_list(client)
    logger.info(processed_files["status"])
    if processed_files["file_list"]:
        for file in processed_files["file_list"]:
            result = insert_data_into_data_warehouse(client, file, conn)
            logger.info(result)
    return {
        "status": "success",
        "message": f'added data from {len(processed_files["file_list"])} tables to data warehouse',
    }
