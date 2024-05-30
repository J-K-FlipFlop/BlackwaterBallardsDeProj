import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from src.transform_lambda.transform_funcs import create_dim_dates


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
    def test_create_dim_date_rtns_df_type_removes_drop_cols_and_adds_sales_cols(
        self, s3_client, file_name1="design", file_name2="last_ran_at"
    ):
        bucket = "blackwater-ingestion-zone"
        filename1 = f"test/data/{file_name1}.csv"
        filename2 = f"test/data/last_ran_at_99/{file_name2}.csv"
        key1 = f"ingested_data/original_data_dump/{file_name1}.csv"
        key2 = f"{file_name2}.csv"

        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = create_dim_dates(s3_client)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]
        for column in column_names:
            assert column in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_create_dim_date_rtns_expected_data_from_dataframe(
        self, s3_client, file_name1="design", file_name2="last_ran_at"
    ):
        bucket = "blackwater-ingestion-zone"
        filename1 = f"test/data/{file_name1}.csv"
        filename2 = f"test/data/last_ran_at_99/{file_name2}.csv"
        key1 = f"ingested_data/original_data_dump/{file_name1}.csv"
        key2 = f"{file_name2}.csv"

        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = create_dim_dates(s3_client)
        assert result["status"] == "success"
        assert list(result["data"]["date_id"][0:10]) == [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-09",
            "2020-01-10",
        ]
        assert list(result["data"]["year"][0:10]) == [
            2020,
            2020,
            2020,
            2020,
            2020,
            2020,
            2020,
            2020,
            2020,
            2020,
        ]
        assert list(result["data"]["month"][0:10]) == [
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
        ]
        assert list(result["data"]["day"][0:10]) == [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
        ]
        assert list(result["data"]["day_of_week"][0:10]) == [
            3,
            4,
            5,
            6,
            7,
            1,
            2,
            3,
            4,
            5,
        ]
        assert list(result["data"]["day_name"][0:10]) == [
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
        ]
        assert list(result["data"]["month_name"][0:10]) == [
            "January",
            "January",
            "January",
            "January",
            "January",
            "January",
            "January",
            "January",
            "January",
            "January",
        ]
        assert list(result["data"]["quarter"][0:10]) == [
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
        ]

    def test_create_dim_date_without_last_ran_at_returns_expected_error(
        self, s3_client, file_name="design"
    ):
        bucket = "blackwater-ingestion-zone"
        filename = f"test/data/{file_name}.csv"
        key = f"ingested_data/original_data_dump/{file_name}.csv"

        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = create_dim_dates(s3_client)
        assert result == {
            "message": "dim date already set",
            "status": "failure",
        }

    def test_create_dim_date_passed_error_seesion_returns_expected_error(
        self, s3_client, file_name1="design", file_name2="last_ran_at"
    ):
        bucket = "blackwater-ingestion-zone"
        filename1 = f"test/data/{file_name1}.csv"
        filename2 = f"test/data/last_ran_at_99/{file_name2}.csv"
        key1 = f"ingested_data/original_data_dump/{file_name1}.csv"
        key2 = f"{file_name2}.csv"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = create_dim_dates(s3_client, session)
        assert result == {
            "message": "something has gone horrifically wrong, check this",
            "status": "failed",
        }
