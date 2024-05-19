from src.extract_lambda.utils import write_csv_to_s3, convert_table_to_dict
from pprint import pprint as pp
from datetime import datetime
import boto3

# .strftime('%Y, %m, %d, %H, %M, %S, %f')
#'2024, 5, 16, 14, 00, 00, 00000'


def update_data_in_bucket(table: str):
    sales_table_info = convert_table_to_dict(table)
    bucket = "blackwater-ingestion-zone"
    runtime_key = f"last_ran_at.csv"
    try:
        get_previous_runtime = boto3.resource("s3").Object(bucket, runtime_key)
        previous_lambda_runtime_uncut = (
            get_previous_runtime.get()["Body"].read().decode("utf-8")
        )
        # if structure of blackwater-ingestion-zone/last_ran_at changes slice on line below will likely have to be updated too
        previous_lambda_runtime = datetime.strptime(
            previous_lambda_runtime_uncut[12:-2], "%Y-%m-%d %H:%M:%S.%f"
        )
    except:
        previous_lambda_runtime = datetime(1999, 12, 31, 23, 59, 59, 999999)
    pp(previous_lambda_runtime)
    new_items = []
    current_lambda_runtime = datetime.now()
    for item in sales_table_info:
        item_latest_update = item["last_updated"]
        if item_latest_update > previous_lambda_runtime:
            new_items.append(item)
    session = boto3.session.Session()
    # ^^^ change if run on Lambda or not - Lambda does not require access key entries

    data = new_items
    key = f"update_test/{current_lambda_runtime}/{table}.csv"
    write_csv_to_s3(session=session, data=data, bucket=bucket, key=key)

    current_runtime = [{"last_ran_at": current_lambda_runtime}]

    write_csv_to_s3(
        session=session, data=current_runtime, bucket=bucket, key=runtime_key
    )


update_data_in_bucket("sales_order")
