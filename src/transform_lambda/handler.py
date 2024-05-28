from src.transform_lambda.transform_funcs import (
    convert_counterparty,
    convert_currency,
    convert_design,
    convert_location,
    convert_sales_order,
    convert_staff,
    create_dim_date,
)
from src.transform_lambda.utils import (
    write_parquet_data_to_s3,
    read_latest_changes,
)
import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    # x = read_latest_changes(client)
    session = boto3.session.Session()
    client = session.client("s3")
    # print(x)
    curr = convert_currency(client, session)
    cp = convert_counterparty(client, session)
    des = convert_design(client, session)
    loc = convert_location(client, session)
    stf = convert_staff(client, session)
    sales = convert_sales_order(client, session)
    # print(loc)
    # print(sales)

    if sales["status"] == "success":
        date = create_dim_date(sales["data"])
    else:
        date = {}
        date["status"] = "failed"

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

    # s3.copyObject({
    #     Bucket: "blackwater-ingestion-zone",
    #     Key: "last_ran_at.csv"
    #     CopySource:
    # })

    message = f"Updated {counter} tables"
    print(message)
    logging.info(message)

    # convert_design()
    # convert_location()
    # cp = convert_counterparty()
    # convert_staff()
    # convert_sales_order()
    # create_dim_date()


# lambda_handler("dum", "my")
