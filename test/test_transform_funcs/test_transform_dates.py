import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from src.transform_lambda.transform_funcs import convert_sales_order, create_dim_date

@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")
       

class TestConvertDates:
    def test_create_dim_date_rtns_df_type_removes_drop_cols_and_adds_sales_cols(self, s3_client, file_name="sales_order"):
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
        assert date_result["status"] == "success"
        assert isinstance(date_result["data"], pd.DataFrame)
        column_names = ['dates', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
        for column in column_names:
            assert column in date_result["data"].columns
            assert len(date_result["data"].columns) == len(column_names)

    def test_create_dim_date_rtns_correct_data_in_df(self, s3_client, file_name="sales_order"):
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
        assert list(date_result['data']['dates'][0:10]) == ['2022-11-03', '2022-11-04', '2022-11-07', '2022-11-09', '2022-11-10', '2022-11-11', '2022-11-15', '2022-11-16', '2022-11-17', '2022-11-18']
        assert list(date_result['data']['year'][0:10]) == [2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022]
        assert list(date_result['data']['month'][0:10]) == [11, 11, 11, 11, 11, 11, 11, 11, 11, 11]
        assert list(date_result['data']['day'][0:10]) == [3, 4, 7, 9, 10, 11, 15, 16, 17, 18]
        assert list(date_result['data']['day_of_week'][0:10]) == [3, 4, 0, 2, 3, 4, 1, 2, 3, 4]
        assert list(date_result['data']['day_name'][0:10]) == ['Thursday', 'Friday', 'Monday', 'Wednesday', 'Thursday', 'Friday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        assert list(date_result['data']['month_name'][0:10]) == ['November', 'November', 'November', 'November', 'November', 'November', 'November', 'November', 'November', 'November']
        assert list(date_result['data']['quarter'][0:10]) ==[4, 4, 4, 4, 4, 4, 4, 4, 4, 4]

    def test_create_dim_date_without_req_file_returns_expected_error(self, s3_client, file_name="counterparty"):
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
        #returns key error because of missing dependency of convert sales order pre-requisite (no data in result)
        with pytest.raises(KeyError) as e:
            create_dim_date(result['data'])
            e.response == 'data'
    
    def test_convert_sales_order_without_req_bucket_returns_expected_error(self, s3_client, file_name="sales_order"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{filename}.csv"
        bucket = "blackwater-congestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = convert_sales_order(s3_client, session)
        #returns key error because of missing dependency of convert sales order pre-requisite (no data in result)
        with pytest.raises(KeyError) as e:
            create_dim_date(result['data'])
            e.response == 'data'