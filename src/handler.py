from src.connection import connect_to_db
import csv
from pg8000.exceptions import DatabaseError


def connect_to_db_table(table):
    try:
        conn = connect_to_db()
        result = conn.run(f"Select * From :table;", table=table)
        columns = [col["name"] for col in conn.columns]
        sales_order_dicts = [dict(zip(columns, a)) for a in result]
        return sales_order_dicts
    except DatabaseError:
        error_message = f'relation "{table}" does not exist'
        return {"status": "Failed", "message": error_message}


def create_csv_data(table, formatted_list):
    with open(f"src/csv/{table}.csv", "w", newline="") as f:
        sales_order_csv = csv.DictWriter(f, formatted_list[0].keys())
        sales_order_csv.writeheader()
        for dicts in formatted_list:
            sales_order_csv.writerow(dicts)
    return f"data from {table} written to src/csv/{table}.csv"


# result = connect_to_db_table("staff")
# if result[0].get("status","") == "Failed":
#     print(result)
# else:
#     create_csv_data("staff", result)
