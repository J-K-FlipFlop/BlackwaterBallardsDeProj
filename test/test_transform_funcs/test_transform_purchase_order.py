import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from numpy import nan
from src.transform_lambda.transform_funcs import convert_purchase_order

@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestConvertLocation:
    def test_convert_purchaseorder_rtns_df_type_removes_drop_cols_and_adds_time_and_date_col(self, s3_client, file_name="purchase_order"):
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
        
        result = convert_purchase_order(s3_client, session)

        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['purchase_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'staff_id', 'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated']
        for column in removed_columns:
            assert column not in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_convert_purchase_order_without_req_file_returns_expected_error(self, s3_client, file_name="address"):
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
        result = convert_purchase_order(s3_client, session)
        assert result['status'] == "failure"
        assert str(result['message']) == f"No files Found on: s3://{bucket}/ingested_data/{timestamp}/purchase_order.csv."

    def test_convert_purchase_order_without_req_bucket_returns_expected_error(self, s3_client, file_name="purchase_order"):
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
        result = convert_purchase_order(s3_client, session)
        #error message on utils line 43-47 need improvement?
        assert result['status'] == "failure"
        assert result['timestamp'] == ""

    def test_convert_purchaseorder_rtns_expected_data_from_dataframe(self, s3_client, file_name="purchase_order"):
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
        
        result = convert_purchase_order(s3_client, session)
        print(result, '<---- RESULTSSSSS')
        assert list(result['data']['purchase_order_id'][0:2]) == [1, 2]
        assert list(result['data']['created_date'][0:2]) == ['2022-11-03', '2022-11-03']
        assert list(result['data']['created_time'][0:2]) == ['14:20:52.187', '14:20:52.186']
        assert list(result['data']['last_updated_date'][0:2]) == ['2022-11-03', '2022-11-03']
        assert list(result['data']['last_updated_time'][0:2]) == ['14:20:52.187', '14:20:52.186']
        assert list(result['data']['staff_id'][0:2]) == [12, 20]
        assert list(result['data']['counterparty_id'][0:2]) == [11, 17]
        assert list(result['data']['item_code'][0:2]) == ['ZDOI5EA', 'QLZLEXR']
        assert list(result['data']['item_quantity'][0:2]) == [371, 286]
        assert list(result['data']['item_unit_price'][0:2]) == [361.39, 199.04]
        assert list(result['data']['currency_id'][0:2]) == [2, 2]
        assert list(result['data']['agreed_delivery_date'][0:2]) == ['2022-11-09', '2022-11-04']
        assert list(result['data']['agreed_payment_date'][0:2]) == ['2022-11-07', '2022-11-07']
        assert list(result['data']['agreed_delivery_location_id'][0:2]) == [6, 8]