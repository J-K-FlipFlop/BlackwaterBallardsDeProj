from src.connection import connect_to_db
import csv

def connect_to_db_table(table):
    conn = connect_to_db()
    result = conn.run(f'Select * From {table} LIMIT 10;')
    columns = [col['name'] for col in conn.columns]
    sales_order_dicts = []
    with open(f'src/csv/{table}.csv', 'w', newline='') as f:
        for a in result:
            new_a = dict(zip(columns, a))
            print(new_a)
            sales_order_dicts.append(new_a)
        sales_order_csv = csv.DictWriter(f, new_a.keys())
        sales_order_csv.writeheader()
        for dicts in sales_order_dicts:
            sales_order_csv.writerow(dicts)
    return f"data from {table} written to src/csv/{table}.csv"
