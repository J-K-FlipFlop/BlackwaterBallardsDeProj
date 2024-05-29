import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from src.transform_lambda.transform_funcs import convert_currency, convert_staff

@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")
       

class TestConvertStaff:
    def test_convert_staff_rtns_df_type_removes_drop_cols_and_adds_dept_cols(self, s3_client, file_name1="staff", file_name2="department", file_name3="last_ran_at"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        key1 = f"ingested_data/{timestamp}/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key2 = f"ingested_data/original_data_dump/{file_name2}.csv"
        filename3 = f"test/data/{file_name3}.csv"
        key3 = f"{file_name3}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        s3_client.upload_file(Filename=filename3, Bucket=bucket, Key=key3)
        
        result = convert_staff(s3_client, session)
        # assert result["message"] == "marge"
        print(result)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated', 'manager', 'department_id']
        for column in removed_columns:
            assert column not in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_convert_staff_rtns_expected_data_from_dataframe(self, s3_client, file_name1="staff", file_name2="department"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        key1 = f"ingested_data/{timestamp}/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key2 = f"ingested_data/original_data_dump/{file_name2}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        
        result = convert_staff(s3_client, session)
        assert list(result['data']['staff_id'][0:10]) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        assert list(result['data']['first_name'][0:10]) == ['Jeremie', 'Deron', 'Jeanette', 'Ana', 'Magdalena', 'Korey', 'Raphael', 'Oswaldo', 'Brody', 'Jazmyn']
        assert list(result['data']['last_name'][0:10]) == ['Franey', 'Beier', 'Erdman', 'Glover', 'Zieme', 'Kreiger', 'Rippin', 'Bergstrom', 'Ratke', 'Kuhn']
        assert list(result['data']['department_name'][0:10]) == ['Purchasing', 'Facilities', 'Facilities', 'Production', 'HR', 'Production', 'Purchasing', 'Communications', 'Purchasing', 'Purchasing']
        assert list(result['data']['location'][0:10]) == ['Manchester', 'Manchester', 'Manchester', 'Leeds', 'Leeds', 'Leeds', 'Manchester', 'Leeds', 'Manchester', 'Manchester']
        assert list(result['data']['email_address'][0:10]) == ['jeremie.franey@terrifictotes.com', 'deron.beier@terrifictotes.com', 'jeanette.erdman@terrifictotes.com', 'ana.glover@terrifictotes.com', 'magdalena.zieme@terrifictotes.com', 'korey.kreiger@terrifictotes.com', 'raphael.rippin@terrifictotes.com', 'oswaldo.bergstrom@terrifictotes.com', 'brody.ratke@terrifictotes.com', 'jazmyn.kuhn@terrifictotes.com']
        
    def test_convert_staff_without_req_file_returns_expected_error(self, s3_client, file_name1="design", file_name2="department"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key1 = f"ingested_data/{timestamp}/{filename1}.csv"
        key2 = f"ingested_data/{timestamp}/{filename2}.csv"
        bucket = "blackwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_staff(s3_client, session)
        assert result['status'] == "failure"
        assert str(result['message']) == f"No files Found on: s3://{bucket}/ingested_data/{timestamp}/staff.csv."

    def test_convert_staff_without_req_bucket_returns_expected_error(self, s3_client, file_name1="staff", file_name2="department"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key1 = f"update_test/{timestamp}/{filename1}.csv"
        key2 = f"update_test/{timestamp}/{filename2}.csv"
        bucket = "brownwater-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_currency(s3_client, session)
        #error message on utils line 43-47 need improvement?
        assert result['status'] == "failure"
        assert result['timestamp'] == ""
        
        
        
    