import pytest
import boto3
import os
import pandas as pd
import awswrangler as wr
from moto import mock_aws
from src.transform_lambda.utils import (
    read_latest_changes,
    get_data_from_ingestion_bucket,
    write_parquet_data_to_s3,
)
from botocore.exceptions import ClientError


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestReadLatestChanges:
    def test_function_returns_a_file_name_and_datetime(self, s3_client):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"ingested_data/{timestamp}/staff.csv"
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = read_latest_changes(s3_client)
        assert result["timestamp"] == timestamp
        assert result["file_list"] == [
            "ingested_data/2024-05-20 12:10:03.998128/staff.csv"
        ]

    def test_function_returns_multiple_filenames_if_same_max_timestamp(self, s3_client):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"ingested_data/{timestamp}/staff.csv"
        key2 = f"ingested_data/{timestamp}/currency.csv"
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key2)
        key3 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key3)
        result = read_latest_changes(s3_client)
        assert result["timestamp"] == timestamp
        assert result["file_list"] == [
            "ingested_data/2024-05-20 12:10:03.998128/staff.csv",
            "ingested_data/2024-05-20 12:10:03.998128/currency.csv",
        ]

    def test_function_returns_latest_filename_if_bucket_contains_multiple_folders(
        self, s3_client
    ):
        timestamp = "2024-05-20 12:10:03.998128"
        timestamp2 = "2024-05-19 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"ingested_data/{timestamp}/staff.csv"
        key2 = f"ingested_data/{timestamp2}/currency.csv"
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key2)
        key3 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key3)
        result = read_latest_changes(s3_client)
        assert result["timestamp"] == timestamp
        assert result["file_list"] == [
            "ingested_data/2024-05-20 12:10:03.998128/staff.csv",
        ]


class TestGetFileContents:
    def test_function_returns_pandas_dataframe(self, s3_client):
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"ingested_data/{timestamp}/staff.csv"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        result = get_data_from_ingestion_bucket(
            key=timestamp, filename=key.split("/")[-1], session=session
        )
        assert isinstance(result["data"], pd.DataFrame)

    def test_data_in_result_in_expected_format_and_type(self, s3_client):
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/TestGetFileContents.csv"
        key = f"ingested_data/{timestamp}/staff.csv"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        result = get_data_from_ingestion_bucket(
            key=timestamp, filename=key.split("/")[-1], session=session
        )
        assert len(result["data"].columns) == 7
        expected = [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ]
        columns = result["data"].columns.values
        for col in columns:
            assert col in expected
        assert len(result["data"]) == 5

    def test_missing_missing_bucket_raises_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        input_key = "ingested_data/2024-05-20 12:10:03.998128/staff.csv"
        result = get_data_from_ingestion_bucket(input_key, session)
        assert result["status"] == "failure"
        assert result["message"]["Error"]["Code"] == "NoSuchBucket"

    def test_missing_missing_bucket_raises_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        input_key = "ingested_data/2024-05-20 12:10:03.998128/staff.csv"
        key = input_key.split("/")[1]
        filename = input_key.split("/")[-1]
        result = get_data_from_ingestion_bucket(
            key=key, filename=filename, session=session
        )
        assert result["status"] == "failure"
        assert (
            str(result["message"]) == f"No files Found on: s3://{bucket}/{input_key}."
        )


class TestWriteParquet:
    def test_function_writes_to_s3_bucket(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        input_table = "staff"
        timestamp = "2024-05-20 12:10:03.998128"
        dataframe = pd.read_csv("test/data/dummy_csv.csv")
        write_parquet_data_to_s3(dataframe, input_table, session, timestamp)
        result = s3_client.list_objects_v2(Bucket=bucket)
        assert result["Contents"][0]["Key"] == f"{timestamp}/{input_table}.parquet"

    def test_function_writes_correct_data_to_s3_bucket(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        input_table = "staff"
        timestamp = "2024-05-20 12:10:03.998128"
        dataframe = pd.read_csv("test/data/TestGetFileContents.csv")
        write_parquet_data_to_s3(dataframe, input_table, session, timestamp)
        result = wr.s3.read_parquet(
            path=f"s3://{bucket}/{timestamp}/{input_table}.parquet",
        )
        assert result.to_dict(orient="index") == {
            0: {
                "counterparty_id": 1,
                "counterparty_legal_name": "Fahey and Sons",
                "legal_address_id": 15,
                "commercial_contact": "Micheal Toy",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
            1: {
                "counterparty_id": 2,
                "counterparty_legal_name": "Leannon, Predovic and Morar",
                "legal_address_id": 28,
                "commercial_contact": "Melba Sanford",
                "delivery_contact": "Jean Hane III",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
            2: {
                "counterparty_id": 3,
                "counterparty_legal_name": "Armstrong Inc",
                "legal_address_id": 2,
                "commercial_contact": "Jane Wiza",
                "delivery_contact": "Myra Kovacek",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
            3: {
                "counterparty_id": 4,
                "counterparty_legal_name": "Kohler Inc",
                "legal_address_id": 29,
                "commercial_contact": "Taylor Haag",
                "delivery_contact": "Alfredo Cassin II",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
            4: {
                "counterparty_id": 5,
                "counterparty_legal_name": "Frami, Yundt and Macejkovic",
                "legal_address_id": 22,
                "commercial_contact": "Homer Mitchell",
                "delivery_contact": "Ivan Balistreri",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
        }

    def test_missing_bucket_raises_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-processed-zone"
        input_table = "staff"
        timestamp = "2024-05-20 12:10:03.998128"
        dataframe = pd.read_csv("test/data/dummy_csv.csv")
        result = write_parquet_data_to_s3(dataframe, input_table, session, timestamp)
        assert result["status"] == "failure"
        assert result["message"]["Error"]["Code"] == "NoSuchBucket"

    def test_passing_data_in_incorrect_format_throws_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        input_table = "staff"
        timestamp = "2024-05-20 12:10:03.998128"
        data = "string not dataframe"
        result = write_parquet_data_to_s3(data, input_table, session, timestamp)
        assert result["status"] == "failure"
        assert (
            result["message"]
            == "Data is in wrong format <class 'str'> is not a pandas dataframe"
        )
