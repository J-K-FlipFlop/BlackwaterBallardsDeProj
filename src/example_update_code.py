from handler import connect_to_db_table
from pprint import pprint as pp
from datetime import datetime
from src.lambda_extract_function import write_csv_to_s3
import boto3

# .strftime('%Y, %m, %d, %H, %M, %S, %f')
#'2024, 5, 16, 14, 00, 00, 00000'

def update_data_in_bucket(table: str):
    sales_table_info = connect_to_db_table(table)


    previous_lambda_runtime = datetime(year=2024, month=5, day=16, hour=14, minute=00, second=00, microsecond=00000)
    
    
    new_items = []
    current_lambda_runtime = datetime.now()
    for item in sales_table_info:
        item_latest_update = item['last_updated']
        if item_latest_update > previous_lambda_runtime:
            new_items.append(item)
    session = boto3.session.Session()
    # ^^^ change if run on Lambda or not - Lambda does not require access key entries
    bucket = "blackwater-ingestion-zone"
    data = new_items
    key = f'update_test/{current_lambda_runtime}/{table}.csv'
    write_csv_to_s3(session=session, data=data, bucket=bucket, key=key)
    
    current_runtime = [{"last_ran_at" :current_lambda_runtime}]
    runtime_key = f'last_ran_at.csv'
    write_csv_to_s3(session=session, data= current_runtime, bucket=bucket, key=runtime_key)



update_data_in_bucket('sales_order')