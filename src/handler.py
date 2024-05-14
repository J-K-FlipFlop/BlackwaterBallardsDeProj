from src.connection import connect_to_db
from src.utils.dict_formatter import format_db_table_to_dict
import csv

def connect_to_db_table(table):
    conn = connect_to_db()
    columns = conn.run(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';")
    result = conn.run(f'Select * From {table};')
    sales_order_dicts = []
    with open(f'src/csv/{table}.csv', 'w', newline='') as f:
        for a in result:
            new_a = format_db_table_to_dict(columns, a)
            sales_order_dicts.append(new_a)
        sales_order_csv = csv.DictWriter(f, new_a.keys())
        sales_order_csv.writeheader()
        for dict in sales_order_dicts:
            sales_order_csv.writerow(dict)

connect_to_db_table('sales_order')
connect_to_db_table('design')
connect_to_db_table('payment')


