import pytest
import boto3
import os
from moto import mock_aws
from src.extract_lambda.handler import lambda_handler


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestLambdaHandler:
    def test_handler_returns_false_message(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        result = lambda_handler("unused", "unused2", session)
        assert result == {
            "success": "false",
            "message": "The specified bucket does not exist",
        }

    def test_handler_returns_true_message(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket="blackwater-ingestion-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = lambda_handler("unused", "unused2", session)
        assert result == {"success": "true", "message": "written to bucket"}

    def test_handler_writes_correct_number_of_files_to_bucket(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket="blackwater-ingestion-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        lambda_handler("unused", "unused2", session)
        response = s3_client.list_objects_v2(Bucket="blackwater-ingestion-zone")
        assert len(response["Contents"]) == 11

    def test_handler_writes_data_to_each_file(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket="blackwater-ingestion-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        lambda_handler("unused", "unused2", session)
        response = s3_client.list_objects_v2(Bucket="blackwater-ingestion-zone")
        for file in response["Contents"]:
            assert file["Size"] > 100
