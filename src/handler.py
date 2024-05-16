from src.connection import connect_to_db
import csv
from pg8000.exceptions import DatabaseError
import pandas as pd
import re


def connect_to_db_table(table):
    table = sql_security(table)
    try:
        conn = connect_to_db()
        result = conn.run(f"Select * From {table};")
        columns = [col["name"] for col in conn.columns]
        sales_order_dicts = [dict(zip(columns, a)) for a in result]
        return sales_order_dicts
    except DatabaseError:
        error_message = f'relation "{table}" does not exist'
        return {"status": "Failed", "message": error_message}


# def create_csv_data(table, formatted_list):
#     table = sql_security(table)
#     with open(f"src/csv/{table}.csv", "w", newline="") as f:
#         sales_order_csv = csv.DictWriter(f, formatted_list[0].keys())
#         sales_order_csv.writeheader()
#         for dicts in formatted_list:
#             sales_order_csv.writerow(dicts)
#     return f"data from {table} written to src/csv/{table}.csv"

def sql_security(table):
    conn = connect_to_db()
    table_names_unfiltered = conn.run("SELECT TABLE_NAME FROM totesys.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    regex = re.compile('(^pg_)|(^sql_)|(^_)')
    table_names_filtered = [item[0] for item in table_names_unfiltered if not regex.search(item[0])]
    if table in table_names_filtered:
        return table
    else:
        raise DatabaseError("Table not found - do not start a table name with pg_, sql_ or _")

# connect_to_db_table("pg_Harry")

    
    


# result = connect_to_db_table("staff")
# print(result)

# df = pd.DataFrame(result)
# df.to_csv("test.csv", index=False)


# if result[0].get("status","") == "Failed":
#     print(result)
# else:
#     create_csv_data("staff", result)
