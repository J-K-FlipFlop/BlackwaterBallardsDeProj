from handler import connect_to_db_table
from pprint import pprint as pp
from datetime import datetime

def update_data_in_bucket():
    sales_table_info = connect_to_db_table('sales_order')
    previous_update_time = '2024, 5, 16, 14, 00, 00, 00000'
    for item in sales_table_info:
        last_update_time = item['last_updated'].strftime('%Y, %m, %d, %H, %M, %S, %f')
        if last_update_time

update_data_in_bucket()