import pytest
import boto3
import os
from moto import mock_aws
from src.upload import lambda_handler, write_to_s3, write_csv_to_s3
import datetime


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


class TestWriteCsvToS3:
    def test_csv_file_is_written_to_bucket(self, s3_client):
        session = boto3.session.Session(
        aws_access_key_id="test", aws_secret_access_key="test"
    )
        data = [
            {
                "staff_id": 1,
                "first_name": "Jeremie",
                "last_name": "Franey",
                "department_id": 2,
                "email_address": "jeremie.franey@terrifictotes.com",
                "created_at": datetime.datetime(
                    2022, 11, 3, 14, 20, 51, 563000
                ),
                "last_updated": datetime.datetime(
                    2022, 11, 3, 14, 20, 51, 563000
                ),
            },
            {
                "staff_id": 2,
                "first_name": "Deron",
                "last_name": "Beier",
                "department_id": 6,
                "email_address": "deron.beier@terrifictotes.com",
                "created_at": datetime.datetime(
                    2022, 11, 3, 14, 20, 51, 563000
                ),
                "last_updated": datetime.datetime(
                    2022, 11, 3, 14, 20, 51, 563000
                ),
            },
        ]
        bucket = "bucket-for-my-emotions"
        key = "folder/file.csv"

        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = write_csv_to_s3(session, data, bucket, key)
        assert result["message"] == "written to bucket"

