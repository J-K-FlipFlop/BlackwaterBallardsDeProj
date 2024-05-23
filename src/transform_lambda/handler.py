from transform_funcs import (convert_counterparty,
                             convert_currency,
                             convert_design,
                             convert_location,
                             convert_sales_order,
                             convert_staff,
                             create_dim_date)
from utils import write_parquet_data_to_s3, read_latest_changes
import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.session.Session()
client = boto3.client("s3")

def lambda_handler(event, context, session=None, client=None):
    # x = read_latest_changes(client)
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


    counter = 0

    if curr["status"] == "success":
        resp = write_parquet_data_to_s3(curr["data"], "currency", session)
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("currency not written")
        logging.info(curr)
    
    if cp["status"] == "success":
        resp = write_parquet_data_to_s3(cp["data"], "counterparty", session)
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("counterparty not written")
        logging.info(cp)
    
    if des["status"] == "success":
        resp = write_parquet_data_to_s3(des["data"], "design", session)
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("design not written")
        logging.info(des)
    
    if loc["status"] == "success":
        resp = write_parquet_data_to_s3(loc["data"], "location", session)
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("location not written")
        logging.info(loc)
    
    if stf["status"] == "success":
        resp = write_parquet_data_to_s3(stf["data"], "staff", session)
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("staff not written")
        logging.info(stf)
    
    if sales["status"] == "success":
        resp = write_parquet_data_to_s3(sales["data"], "sales", session)
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("sales not written")
        logging.info(sales)
        return sales
    
    if date["status"] == "success":
        resp = write_parquet_data_to_s3(date["data"], "dates", session)
        counter += 1
        print(resp)
        logging.info(resp)
    else:
        print("date not written")
        logging.info(date)
    
    message = f"Updated {counter} tables"
    print(message)
    logging.info(message)


    # convert_design()
    # convert_location()
    # cp = convert_counterparty()
    # convert_staff()
    # convert_sales_order()
    # create_dim_date()

lambda_handler("dum", "my", session, client)