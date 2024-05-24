import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from src.transform_lambda.transform_funcs import convert_sales_order

@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")
       

class TestConvertSales:
    def test_convert_salesorder_rtns_df_type_removes_drop_cols_and_adds_time_and_date_col(self, s3_client, file_name="sales_order"):
        timestamp = "original_data_dump"
        filename = f"test/data/{file_name}.csv"
        key = f"ingested_data/{timestamp}/{file_name}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        
        result = convert_sales_order(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['sales_record_id', 'sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated']
        for column in removed_columns:
            assert column not in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_convert_salesorder_rtns_expected_data_from_dataframe(self, s3_client, file_name="sales_order"):
        timestamp = "original_data_dump"
        filename = f"test/data/{file_name}.csv"
        key = f"ingested_data/{timestamp}/{file_name}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        
        result = convert_sales_order(s3_client, session)
        print(result, '<---- RESULTSSSSS')
        assert list(result['data']['sales_order_id'][0:10]) == [2, 3, 4, 5, 6, 7, 8, 10, 11, 12]
        assert list(result['data']['created_date'][0:10]) == ['2022-11-03', '2022-11-03', '2022-11-03', '2022-11-03', '2022-11-04', '2022-11-04', '2022-11-04', '2022-11-07', '2022-11-07', '2022-11-09']
        assert list(result['data']['created_time'][0:10]) == ['14:20:52.186', '14:20:52.188', '14:20:52.188', '14:20:52.186', '11:37:10.341', '12:57:09.926', '13:45:10.306', '09:07:10.485', '15:53:10.153', '10:20:09.912']
        assert list(result['data']['last_updated_date'][0:10]) == ['2022-11-03', '2022-11-03', '2022-11-03', '2022-11-03', '2022-11-04', '2022-11-04', '2022-11-04', '2022-11-07', '2022-11-07', '2022-11-09']
        assert list(result['data']['last_updated_time'][0:10]) == ['14:20:52.186', '14:20:52.188', '14:20:52.188', '14:20:52.186', '11:37:10.341', '12:57:09.926', '13:45:10.306', '09:07:10.485', '15:53:10.153', '10:20:09.912']
        assert list(result['data']['sales_staff_id'][0:10]) == [19, 10, 10, 18, 13, 11, 11, 16, 14, 8]
        assert list(result['data']['counterparty_id'][0:10]) == [8, 4, 16, 4, 18, 10, 20, 12, 12, 12]
        assert list(result['data']['units_sold'][0:10]) == [42972, 65839, 32069, 49659, 83908, 65453, 20381, 61620, 35227, 7693]
        assert list(result['data']['unit_price'][0:10]) == [3.94, 2.91, 3.89, 2.41, 3.99, 2.89, 2.22, 3.86, 3.41, 3.88]
        assert list(result['data']['currency_id'][0:10]) == [2, 3, 2, 3, 3, 2, 2, 2, 2, 2]
        assert list(result['data']['design_id'][0:10]) == [3, 4, 4, 7, 3, 7, 2, 3, 9, 2]
        assert list(result['data']['agreed_payment_date'][0:10]) == ['2022-11-08', '2022-11-07', '2022-11-07', '2022-11-08', '2022-11-07', '2022-11-09', '2022-11-07', '2022-11-10', '2022-11-13', '2022-11-11']
        assert list(result['data']['agreed_delivery_date'][0:10]) == ['2022-11-07', '2022-11-06', '2022-11-05', '2022-11-05', '2022-11-04', '2022-11-04', '2022-11-06', '2022-11-09', '2022-11-08', '2022-11-13']
        assert list(result['data']['agreed_delivery_location_id'][0:10]) == [8, 19, 15, 25, 17, 28, 8, 20, 13, 15]

    def test_convert_sales_order_without_req_file_returns_expected_error(self, s3_client, file_name="address"):
        timestamp = "original_data_dump"
        filename = f"test/data/{file_name}.csv"
        key = f"ingested_data/{timestamp}/{filename}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_sales_order(s3_client, session)
        assert result['status'] == "failure"
        assert str(result['message']) == f"No files Found on: s3://{bucket}/ingested_data/{timestamp}/sales_order.csv."

    def test_convert_sales_order_without_req_bucket_returns_expected_error(self, s3_client, file_name="sales_order"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{filename}.csv"
        bucket = "blackwater-infestation-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = convert_sales_order(s3_client, session)
        #error message on utils line 43-47 need improvement?
        assert result['status'] == "failure"
        assert result['timestamp'] == ""
        


        