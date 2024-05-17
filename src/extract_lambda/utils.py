from src.extract_lambda.connection import connect_to_db
from botocore.exceptions import ClientError
from pg8000.exceptions import DatabaseError
import logging
import pandas as pd
import re
import awswrangler as wr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def convert_table_to_dict(table):
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
    finally:
        conn.close()

def sql_security(table):
    conn = connect_to_db()
    table_names_unfiltered = conn.run("SELECT TABLE_NAME FROM totesys.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    regex = re.compile('(^pg_)|(^sql_)|(^_)')
    table_names_filtered = [item[0] for item in table_names_unfiltered if not regex.search(item[0])]
    if table in table_names_filtered:
        return table
    else:
        raise DatabaseError("Table not found - do not start a table name with pg_, sql_ or _")

def write_to_s3(client, data, bucket, key):
    """Helper to write material to S3."""
    body = data
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body)
        return {"status": "success", "message": "written to bucket"}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        return {"status": "failed", "message": c.response["Error"]["Message"]}
    
def write_csv_to_s3(session, data, bucket, key):
    try:
        response =  wr.s3.to_csv(
            df=pd.DataFrame(data),
            path=f's3://{bucket}/{key}',
            boto3_session=session,
            index=False
            )
        return {"success": True, "message": "written to bucket"}
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        response = {"success": False, "message": c.response["Error"]["Message"]}
        # print(response)
        return response