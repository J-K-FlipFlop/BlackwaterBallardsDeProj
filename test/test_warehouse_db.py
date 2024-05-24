from pprint import pprint as pp
from pg8000.native import Connection, DatabaseError
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import pytest
from src.load_lambda.utils import insert_data_into_data_warehouse
from moto import mock_aws
import boto3

load_dotenv()


psql_user = os.getenv("psql_username")
if psql_user == None:
    psql_user = "_"


psql_password = os.getenv("psql_password")
if psql_password == None:
    psql_password = "_"



@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


def root_warehouse_db() -> Connection:
    conn = Connection(user=psql_user, password=psql_password, port=5432, host="localhost")
    conn.run("DROP DATABASE IF EXISTS postgres;")
    conn.run("CREATE DATABASE postgres;")
    conn = Connection(user=psql_user, password=psql_password, database="postgres", port=5432, host="localhost")
    return conn

#table_name:str, data, 

def seed_warehouse_db(connection:Connection):
    connection.run("""CREATE TABLE
        dim_date(
            date_id DATE PRIMARY KEY NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            day_of_week INT NOT NULL,
            month_name VARCHAR NOT NULL,
            quarter INT NOT NULL
            );""")
    connection.run("""CREATE TABLE
        dim_staff(
            staff_id INT PRIMARY KEY NOT NULL,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            department_name VARCHAR NOT NULL,
            location VARCHAR NOT NULL,
            email_address VARCHAR NOT NULL
            );""")
    connection.run("""CREATE TABLE
        dim_currency(
            currency_id INT PRIMARY KEY NOT NULL,
            currency_code VARCHAR NOT NULL,
            currency_name VARCHAR NOT NULL
            );""")
    connection.run("""CREATE TABLE
        dim_design(
            design_id INT PRIMARY KEY NOT NULL,
            design_name VARCHAR NOT NULL,
            file_location VARCHAR NOT NULL,
            file_name VARCHAR NOT NULL
            );""")
    connection.run("""CREATE TABLE
        dim_location(
            location_id INT PRIMARY KEY NOT NULL,
            address_line_1 VARCHAR NOT NULL,
            address_line_2 VARCHAR,
            district VARCHAR,
            city VARCHAR NOT NULL,
            postal_code VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            phone VARCHAR NOT NULL
            );""")
    connection.run("""CREATE TABLE
        dim_counterparty(
            counterparty_id INT PRIMARY KEY NOT NULL,
            counterparty_legal_name VARCHAR NOT NULL,
            counterparty_legal_address_line_1 VARCHAR NOT NULL,
            counterparty_legal_address_line2 VARCHAR NOT NULL,
            counterparty_legal_district VARCHAR NOT NULL,
            counterparty_legal_city VARCHAR NOT NULL,
            counterparty_legal_postal_code VARCHAR NOT NULL,
            counterparty_legal_country VARCHAR NOT NULL,
            counterparty_legal_phone_number VARCHAR NOT NULL
            );""")
    connection.run("""CREATE TABLE
        fact_sales_order(
            sales_record_id SERIAL PRIMARY KEY,
            sales_order_id INT NOT NULL,
            created_date DATE NOT NULL REFERENCES dim_date(date_id),
            created_time TIME NOT NULL,
            last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
            last_updated_time TIME NOT NULL,
            sales_staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
            counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
            units_sold INT NOT NULL,
            unit_price NUMERIC(10,2) NOT NULL,
            currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
            design_id INT NOT NULL REFERENCES dim_design(design_id),
            agreed_payment_date DATE NOT NULL REFERENCES dim_date(date_id),
            agreed_delivery_date DATE NOT NULL REFERENCES dim_date(date_id),
            agreed_delivery_locatiion_id INT NOT NULL REFERENCES dim_location(location_id)
            );""")
    #seeding the database in the right way
    #test_result = conn.run(f"SELECT column_name FROM information_schema.columns where table_name = '{table_name}';")
# connection.run(f"INSERT INTO {table_name} VALUES ({data})")
# result = connection.run(f"SELECT * FROM {table_name}")
# connection.close()
# return result

class TestLoadLambda:
    def test_incorrect_table_name(self):
        with pytest.raises(DatabaseError):
            conn = root_warehouse_db()
            seed_warehouse_db(conn)
            conn.run("SELECT * FROM blimble;")
        conn.close()
    
    def test_correct_table_name_returns_correct_values(self, s3_client):
        conn = root_warehouse_db()
        seed_warehouse_db(conn)
        s3_client.create_bucket(
            Bucket="blackwater-ingestion-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket="blackwater-ingestion-zone", Key=key2)
        s3_client.upload_file(Filename="test/data/parquet/currency.parquet", Bucket="blackwater-processed-zone", Key="2024-05-20 12:10:03.998128/dim_currency.parquet")
        insert_data_into_data_warehouse(s3_client, "2024-05-20 12:10:03.998128/dim_currency.parquet", conn)
        result = conn.run("SELECT * FROM dim_currency;")
        conn.close()
        assert result[0][0] == 1
        assert result[1][1] == "USD"
        assert result[2][2] == "Euros"


    def test_correct_table_name_returns_correct_values_location_parquet(self, s3_client):
        conn = root_warehouse_db()
        seed_warehouse_db(conn)
        s3_client.create_bucket(
            Bucket="blackwater-ingestion-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket="blackwater-ingestion-zone", Key=key2)
        s3_client.upload_file(Filename="test/data/parquet/location.parquet", Bucket="blackwater-processed-zone", Key="2024-05-20 12:10:03.998128/dim_location.parquet")
        insert_data_into_data_warehouse(s3_client, "2024-05-20 12:10:03.998128/dim_location.parquet", conn)
        result = conn.run("SELECT * FROM dim_location;")
        conn.close()
        assert result[0][3] == "Avon"
        assert result[29][6] == 'Falkland Islands (Malvinas)'