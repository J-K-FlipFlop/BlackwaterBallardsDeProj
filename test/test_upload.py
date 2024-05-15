import pytest
import boto3
import os
import moto
from moto import mock_aws
from src.upload import lambda_handler, write_to_s3


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestWriteToS3:
    def test_s3_takes_file(self, s3_client):
        data = "src/data/dummy_file.txt"
        bucket = "bucket-for-my-emotions"
        key = "folder/file.txt"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = write_to_s3(s3_client, data, bucket, key)
        assert result["message"] == "written to bucket"

    def test_s3_uploads_correct_file_content(self, s3_client):
        filepath = "src/data/dummy_file.txt"
        bucket = "bucket-for-my-emotions"
        key = "folder/file.txt"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with open(filepath, "rb") as f:
            data = f
            write_to_s3(s3_client, data, bucket, key)

        response = s3_client.get_object(Bucket=bucket, Key=key)
        print(response)
        assert response["Body"].read().decode("UTF-8") == "hello"

    def test_write_to_s3_fails_when_no_bucket(self, s3_client):
        data = "src/data/dummy_file.txt"
        bucket = "bucket-for-my-emotions"
        key = "folder/file.txt"
        result = write_to_s3(s3_client, data, bucket, key)
        assert result["message"] == "The specified bucket does not exist"

    def test_write_to_s3_works_with_csv(self, s3_client):
        filepath = "src/data/dummy_csv.csv"
        bucket = "bucket-for-my-emotions"
        key = "folder/file.txt"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with open(filepath, "rb") as f:
            data = f
            write_to_s3(s3_client, data, bucket, key)

        response = s3_client.get_object(Bucket=bucket, Key=key)
        print(response)
        assert (
            response["Body"].read().decode("UTF-8")
            == "hello, collumn2, the_roman_empire, homer_simpson"
        )
