import pytest
import boto3
import os
import datetime
from moto import mock_aws
from src.extract_lambda.utils import write_csv_to_s3, convert_table_to_dict
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


def test_function_returns_a_list_of_dicts():
    table = "sales_order"
    result = convert_table_to_dict(table)
    assert isinstance(result, list)
    for r in result:
        assert isinstance(r, dict)


def test_header_count_equal_to_result_count():
    table = "staff"
    col_headers = [
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    ]
    result = convert_table_to_dict(table)
    assert len(result[0]) == len(col_headers)


def test_returns_error_message_if_table_name_not_found():
    table = "dog"
    with pytest.raises(DatabaseError):
        convert_table_to_dict(table)


def test_sql_statement_not_vulnerable_to_injection():
    table = "staff; drop table staff;"
    with pytest.raises(DatabaseError):
        convert_table_to_dict(table)


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

    def test_csv_uploads_correct_file_content(self, s3_client):

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
        write_csv_to_s3(session, data, bucket, key)

        response = s3_client.get_object(Bucket=bucket, Key=key)
        output = response["Body"].read().decode("UTF-8")
        assert (
            output
            == "staff_id,first_name,last_name,department_id,email_address,created_at,last_updated\n1,Jeremie,Franey,2,jeremie.franey@terrifictotes.com,2022-11-03 14:20:51.563,2022-11-03 14:20:51.563\n2,Deron,Beier,6,deron.beier@terrifictotes.com,2022-11-03 14:20:51.563,2022-11-03 14:20:51.563\n"
        )

    def test_write_fails_when_bucket_not_found(self, s3_client):
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

        result = write_csv_to_s3(session, data, bucket, key)
        assert result["message"] == "The specified bucket does not exist"
