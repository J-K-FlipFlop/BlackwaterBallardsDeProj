import pytest
import boto3
import os
import datetime
from moto import mock_aws
from src.load_lambda.utils import get_data_from_processed_zone
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

class TestGetProcessedData:

    def test_output_parquet_type(self, s3_client):
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        key = 'dummy_parquet.parquet'