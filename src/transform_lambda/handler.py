from src.transform_lambda.transform_funcs import (
    convert_counterparty,
    convert_currency,
    convert_design,
    convert_location,
    convert_sales_order,
    convert_staff,
    create_dim_dates,
    convert_purchase_order
)
from src.transform_lambda.utils import (
    write_parquet_data_to_s3,
    read_latest_changes,
)
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    session = boto3.session.Session(region_name='eu-west-2')
    client = session.client("s3")
    try:
        cw_client = boto3.client('cloudwatch', region_name='eu-west-2')
        response = cw_client.set_alarm_state(
            AlarmName='AlertTransformLambdaErrors',
            StateValue='OK',
            StateReason='Reset alarm prior to running transform lambda'
        )
        logger.info(response)
    except ClientError as e:
        response = e.response
        logger.error(response)
    
    curr = convert_currency(client, session)
    cp = convert_counterparty(client, session)
    des = convert_design(client, session)
    loc = convert_location(client, session)
    stf = convert_staff(client, session)
    sales = convert_sales_order(client, session)
    purchases = convert_purchase_order(client, session)
    date = create_dim_dates(client)

    counter = 0
    timestamp = read_latest_changes(client)["timestamp"]

    if curr["status"] == "success":
        resp = write_parquet_data_to_s3(
            curr["data"], "dim_currency", session, timestamp=timestamp
        )
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("currency not written")
        logging.info(curr)

    if cp["status"] == "success":
        resp = write_parquet_data_to_s3(
            cp["data"], "dim_counterparty", session, timestamp=timestamp
        )
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("counterparty not written")
        logging.info(cp)

    if des["status"] == "success":
        resp = write_parquet_data_to_s3(
            des["data"], "dim_design", session, timestamp=timestamp
        )
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("design not written")
        logging.info(des)

    if loc["status"] == "success":
        resp = write_parquet_data_to_s3(
            loc["data"], "dim_location", session, timestamp=timestamp
        )
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("location not written")
        logging.info(loc)

    if stf["status"] == "success":
        resp = write_parquet_data_to_s3(
            stf["data"], "dim_staff", session, timestamp=timestamp
        )
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("staff not written")
        logging.info(stf)

    if sales["status"] == "success":
        resp = write_parquet_data_to_s3(
            sales["data"], "fact_sales_order", session, timestamp=timestamp
        )
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("sales not written")
        logging.info(sales)

    if date["status"] == "success":
        resp = write_parquet_data_to_s3(
            date["data"], "dim_date", session, timestamp=timestamp
        )
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("date not written")
        logging.info(date)

    if counter > 0:
        s3 = boto3.resource("s3")
        bucket = s3.Bucket("blackwater-processed-zone")
        copy_source = {
            "Bucket": "blackwater-ingestion-zone",
            "Key": "last_ran_at.csv",
        }
        bucket.copy(copy_source, "last_ran_at.csv")

    message = f"Updated {counter} tables"
    print(message)
    logging.info(message)
