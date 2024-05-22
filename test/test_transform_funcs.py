import boto3
import os
from botocore.exceptions import ClientError
from moto import mock_aws
import pytest
import pandas as pd
from src.transform_lambda.transform_funcs import convert_design, convert_currency, convert_staff, convert_location, convert_counterparty, convert_sales_order, create_dim_date

@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")

class TestConvertDesign:
    def test_convert_design_rtns_df_type_and_removes_drop_cols(self, s3_client, file_name="design"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{file_name}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        
        result = convert_design(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['design_id', 'design_name', 'file_location', 'file_name']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated']
        for column in removed_columns:
            assert column not in result["data"].columns

class TestConvertCurrency:
    def test_convert_curr_rtns_df_type_removes_drop_cols_and_adds_curr_name_col(self, s3_client, file_name="currency"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{file_name}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        
        result = convert_currency(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['currency_id', 'currency_code', 'currency_name']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated']
        for column in removed_columns:
            assert column not in result["data"].columns

class TestConvertStaff:
    def test_convert_staff_rtns_df_type_removes_drop_cols_and_adds_dept_cols(self, s3_client, file_name1="staff", file_name2="department"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        key1 = f"update_test/{timestamp}/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key2 = f"update_test/{timestamp}/{file_name2}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        
        result = convert_staff(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated', 'manager', 'department_id']
        for column in removed_columns:
            assert column not in result["data"].columns

class TestConvertLocation:
    def test_convert_curr_rtns_df_type_removes_drop_cols_and_adds_loc_id_col(self, s3_client, file_name="address"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{file_name}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        
        result = convert_location(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated', 'address_id']
        for column in removed_columns:
            assert column not in result["data"].columns

class TestConvertCounterParty:
    def test_convert_counter_rtns_df_type_removes_drop_cols_and_adds_loc_cols(self, s3_client, file_name1="address", file_name2="counterparty"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        key1 = f"update_test/{timestamp}/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key2 = f"update_test/{timestamp}/{file_name2}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        
        result = convert_counterparty(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_postal_code', 'counterparty_legal_phone_number']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated', 'legala_address_id', 'commercial_contact', 'delivery_contact', 'address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
        for column in removed_columns:
            assert column not in result["data"].columns

class TestConvertSales:
    def test_convert_salesorder_rtns_df_type_removes_drop_cols_and_adds_time_and_date_col(self, s3_client, file_name="sales_order"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{file_name}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        
        result = convert_sales_order(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated']
        for column in removed_columns:
            assert column not in result["data"].columns

class TestConvertDates:
    def test_convert_dates_rtns_df_type_removes_drop_cols_and_adds_sales_cols(self, s3_client, file_name="sales_order"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{file_name}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        
        result = convert_sales_order(s3_client, session)
        date_result = create_dim_date(result['data'])
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['dates', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
        for column in column_names:
            assert column in date_result["data"].columns