import pytest
import boto3
import os
import datetime
import pandas as pd
from moto import mock_aws
from src.load_lambda.utils import (
    get_data_from_processed_zone,
    get_latest_processed_file_list,
)
from pg8000.exceptions import DatabaseError


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestGetLatestProcessedFileList:
    def test_function_returns_failure_message_if_bucket_does_not_exist(
        self, s3_client
    ):
        result = get_latest_processed_file_list(s3_client)
        assert result["status"] == "failure"

    def test_function_returns_success_message_when_bucket_exists(
        self, s3_client
    ):
        bucket = "blackwater-processed-zone"
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.csv"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_latest_processed_file_list(s3_client)
        assert result["status"] == "success"

    def test_function_returns_list_of_recent_files(self, s3_client):
        bucket = "blackwater-processed-zone"
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.parquet"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_latest_processed_file_list(s3_client)
        assert result["file_list"] == [
            "2024-05-20 12:10:03.998128/staff.parquet"
        ]

    def test_function_returns_only_files_that_are_in_latest_folder(
        self, s3_client
    ):
        bucket = "blackwater-processed-zone"
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.parquet"
        timestamp2 = "2024-05-18 12:10:03.998128"
        key2 = f"{timestamp2}/staff.parquet"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key2)
        result = get_latest_processed_file_list(s3_client)
        assert result["file_list"] == [
            "2024-05-20 12:10:03.998128/staff.parquet"
        ]

    def test_function_returns_multiple_files_that_are_in_latest_folder(
        self, s3_client
    ):
        bucket = "blackwater-processed-zone"
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.parquet"
        key2 = f"{timestamp}/sales_order.parquet"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key2)
        result = get_latest_processed_file_list(s3_client)
        assert result["file_list"] == [
            "2024-05-20 12:10:03.998128/sales_order.parquet",
            "2024-05-20 12:10:03.998128/staff.parquet",
        ]


class TestGetProcessedData:

    def test_output_is_failure_if_no_bucket(self, s3_client):
        result = get_data_from_processed_zone(client=s3_client, pq_key='test.parquet')
        assert result['status'] == 'failure'

    def test_output_is_dict_containing_pandas_dataframe(self, s3_client):
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "test.parquet"
        bucket = 'blackwater-processed-zone'
        key = 'test.parquet'
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_data_from_processed_zone(client=s3_client, pq_key='test.parquet')
        assert isinstance(result, dict)
        assert isinstance(result['data'], pd.DataFrame)

    def test_dataframe_content_matches_file_content(self, s3_client):
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "test.parquet"
        bucket = 'blackwater-processed-zone'
        key = 'test.parquet'
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_data_from_processed_zone(client=s3_client, pq_key='test.parquet')
        assert len(result['data']) == 2

class TestInsertDataIntoWarehouse:
    def test_(self, s3_client):
        pass