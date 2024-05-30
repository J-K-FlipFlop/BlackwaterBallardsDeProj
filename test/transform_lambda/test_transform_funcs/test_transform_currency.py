import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from src.transform_lambda.transform_funcs import convert_currency


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestConvertCurrency:
    def test_rtns_df_type_removes_drop_cols_and_adds_curr_name_col(
        self, s3_client, file_name="currency"
    ):
        timestamp = "2024-05-20 12:10:03.998128"
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
        filename2 = "test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)

        result = convert_currency(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ["currency_id", "currency_code", "currency_name"]
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ["created_at", "last_updated"]
        for column in removed_columns:
            assert column not in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_convert_currency_rtns_expected_data_from_dataframe(
        self, s3_client, file_name="currency"
    ):
        timestamp = "2024-05-20 12:10:03.998128"
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
        filename2 = "test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)

        result = convert_currency(s3_client, session)
        assert list(result["data"]["currency_id"]) == [1, 2, 3]
        assert list(result["data"]["currency_code"]) == ["GBP", "USD", "EUR"]
        assert list(result["data"]["currency_name"]) == [
            "Pounds",
            "US dollars",
            "Euros",
        ]

    def test_convert_currency_without_req_file_returns_expected_error(
        self, s3_client, file_name="design"
    ):
        timestamp = "2024-05-20 12:10:03.998128"
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
        filename2 = "test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_currency(s3_client, session)
        assert result["status"] == "failure"
        assert (
            str(result["message"])
            == f"No files Found on: s3://{bucket}/ingested_data/{timestamp}/currency.csv."
        )

    def test_convert_currency_without_req_bucket_returns_expected_error(
        self, s3_client, file_name="currency"
    ):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"ingested_data/{timestamp}/{filename}.csv"
        bucket = "blackwater-implosion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        key2 = "last_ran_at.csv"
        filename2 = "test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = convert_currency(s3_client, session)
        assert result["status"] == "failure"
        assert result["timestamp"] == ""
