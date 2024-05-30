import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from src.transform_lambda.transform_funcs import convert_design

@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestConvertDesign:
    def test_convert_design_rtns_df_type_and_removes_drop_cols(self, s3_client, file_name="design"):
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
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_design(s3_client, session)
        # assert result["message"] == "no"
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['design_id', 'design_name', 'file_location', 'file_name']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated']
        for column in removed_columns:
            assert column not in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_convert_design_rtns_expected_data_from_dataframe(self, s3_client, file_name="design"):
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
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_design(s3_client, session)
        assert list(result['data']['design_id'][0:10]) == [8, 51, 50, 69, 16, 54, 55, 10, 57, 41]
        assert list(result['data']['design_name'][0:10]) == ['Wooden', 'Bronze', 'Granite', 'Bronze', 'Soft', 'Plastic', 'Concrete', 'Soft', 'Cotton', 'Granite']
        assert list(result['data']['file_location'][0:10]) == ['/usr', '/private', '/private/var', '/lost+found', '/System', '/usr/ports', '/opt/include', '/usr/share', '/etc/periodic', '/usr/X11R6']
        assert list(result['data']['file_name'][0:10]) == ['wooden-20220717-npgz.json', 'bronze-20221024-4dds.json', 'granite-20220205-3vfw.json', 'bronze-20230102-r904.json', 'soft-20211001-cjaz.json', 'plastic-20221206-bw3l.json', 'concrete-20210614-04nd.json', 'soft-20220201-hzz1.json', 'cotton-20220527-vn4b.json', 'granite-20220125-ifwa.json']

    def test_convert_design_without_req_file_returns_expected_error(self, s3_client, file_name="currency"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{filename}.csv"
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
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_design(s3_client, session)
        assert result['status'] == "failure"
        assert str(result['message']) == f"No files Found on: s3://{bucket}/ingested_data/{timestamp}/design.csv."

    def test_convert_design_without_req_bucket_returns_expected_error(self, s3_client, file_name="design"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename = f"test/data/{file_name}.csv"
        key = f"update_test/{timestamp}/{filename}.csv"
        bucket = "blackwater-indegestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = convert_design(s3_client, session)
        #error message on utils line 43-47 need improvement?
        assert result['status'] == "failure"
        assert result['timestamp'] == ""
       
        result = convert_design(s3_client, session)
    
        
    