import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from numpy import nan
from src.transform_lambda.transform_funcs import convert_location


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
    def test_convert_location_rtns_df_type_removes_drop_cols_and_adds_loc_id_col(
        self, s3_client, file_name="address"
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

        result = convert_location(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ["created_at", "last_updated", "address_id"]
        for column in removed_columns:
            assert column not in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_convert_location_rtns_expected_data_from_dataframe(
        self, s3_client, file_name="address"
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

        result = convert_location(s3_client, session)
        assert list(result["data"]["location_id"][0:10]) == [
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
        assert list(result["data"]["address_line_1"][0:10]) == [
            "6826 Herzog Via",
            "179 Alexie Cliffs",
            "148 Sincere Fort",
            "6102 Rogahn Skyway",
            "34177 Upton Track",
            "846 Kailey Island",
            "75653 Ernestine Ways",
            "0579 Durgan Common",
            "644 Edward Garden",
            "49967 Kaylah Flat",
        ]
        assert list(result["data"]["address_line_2"][0:10]) == [
            nan,
            nan,
            nan,
            nan,
            nan,
            nan,
            nan,
            nan,
            nan,
            "Tremaine Circles",
        ]
        assert list(result["data"]["district"][0:10]) == [
            "Avon",
            nan,
            nan,
            "Bedfordshire",
            nan,
            nan,
            "Buckinghamshire",
            nan,
            "Borders",
            "Bedfordshire",
        ]
        assert list(result["data"]["city"][0:10]) == [
            "New Patienceburgh",
            "Aliso Viejo",
            "Lake Charles",
            "Olsonside",
            "Fort Shadburgh",
            "Kendraburgh",
            "North Deshaun",
            "Suffolk",
            "New Tyra",
            "Beaulahcester",
        ]
        assert list(result["data"]["postal_code"][0:10]) == [
            "28441",
            "99305-7380",
            "89360",
            "47518",
            "55993-8850",
            "08841",
            "02813",
            "56693-0660",
            "30825-5672",
            "89470",
        ]
        assert list(result["data"]["country"][0:10]) == [
            "Turkey",
            "San Marino",
            "Samoa",
            "Republic of Korea",
            "Bosnia and Herzegovina",
            "Zimbabwe",
            "Faroe Islands",
            "United Kingdom",
            "Australia",
            "Democratic Peoples Republic of Korea",
        ]
        assert list(result["data"]["phone"][0:10]) == [
            "1803 637401",
            "9621 880720",
            "0730 783349",
            "1239 706295",
            "0081 009772",
            "0447 798320",
            "1373 796260",
            "8935 157571",
            "0768 748652",
            "4949 998070",
        ]

    def test_convert_location_without_req_file_returns_expected_error(
        self, s3_client, file_name="department"
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
        result = convert_location(s3_client, session)
        assert result["status"] == "failure"
        assert (
            str(result["message"])
            == f"No files Found on: s3://{bucket}/ingested_data/{timestamp}/address.csv."
        )

    def test_convert_location_without_req_bucket_returns_expected_error(
        self, s3_client, file_name="address"
    ):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{filename}.csv"
        bucket = "blackwater-procrastination-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = convert_location(s3_client, session)
        assert result["status"] == "failure"
        assert result["timestamp"] == ""
