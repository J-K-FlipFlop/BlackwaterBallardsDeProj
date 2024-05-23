import boto3
import os
from moto import mock_aws
import pytest
import pandas as pd
from numpy import nan
from src.transform_lambda.transform_funcs import convert_counterparty

@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")
       

class TestConvertCounterParty:
    def test_convert_counter_rtns_df_type_removes_drop_cols_and_adds_loc_cols(self, s3_client, file_name1="address", file_name2="counterparty"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        key1 = f"update_test/{timestamp}/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key2 = f"update_test/{timestamp}/{file_name2}.csv"
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
        
        result = convert_counterparty(s3_client, session)
        assert result["status"] == "success"
        assert isinstance(result["data"], pd.DataFrame)
        column_names = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']
        for column in column_names:
            assert column in result["data"].columns
        removed_columns = ['created_at', 'last_updated', 'legala_address_id', 'commercial_contact', 'delivery_contact', 'address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
        for column in removed_columns:
            assert column not in result["data"].columns
            assert len(result["data"].columns) == len(column_names)

    def test_convert_counter_rtns_correct_data_in_df(self, s3_client, file_name1="address", file_name2="counterparty"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        key1 = f"update_test/{timestamp}/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key2 = f"update_test/{timestamp}/{file_name2}.csv"
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
        
        result = convert_counterparty(s3_client, session)

        assert list(result['data']['counterparty_id'][0:10]) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        assert list(result['data']['counterparty_legal_name'][0:10]) == ['Fahey and Sons', 'Leannon, Predovic and Morar', 'Armstrong Inc', 'Kohler Inc', 'Frami, Yundt and Macejkovic', 'Mraz LLC', 'Padberg, Lueilwitz and Johnson', 'Grant - Lakin', 'Price LLC', 'Bosco - Grant']
        assert list(result['data']['counterparty_legal_address_line_1'][0:10]) == ['605 Haskell Trafficway', '079 Horacio Landing', '179 Alexie Cliffs', '37736 Heathcote Lock', '364 Goodwin Streets', '822 Providenci Spring', '511 Orin Extension', '511 Orin Extension', '34177 Upton Track', '49967 Kaylah Flat']
        assert list(result['data']['counterparty_legal_address_line_2'][0:10]) == ['Axel Freeway', nan, nan, 'Noemy Pines', nan, nan, 'Cielo Radial', 'Cielo Radial', nan, 'Tremaine Circles']
        assert list(result['data']['counterparty_legal_district'][0:10]) == [nan, nan, nan, nan, nan, 'Berkshire', 'Buckinghamshire', 'Buckinghamshire', nan, 'Bedfordshire']
        assert list(result['data']['counterparty_legal_city'][0:10]) == ['East Bobbie', 'Utica', 'Aliso Viejo', 'Bartellview', 'Sayreville', 'Derekport', 'South Wyatt', 'South Wyatt', 'Fort Shadburgh', 'Beaulahcester']
        assert list(result['data']['counterparty_legal_postal_code'][0:10]) == ['88253-4257', '93045', '99305-7380', '42400-5199', '85544-4254', '25541', '04524-5341', '04524-5341', '55993-8850', '89470']
        assert list(result['data']['counterparty_legal_country'][0:10]) == ['Heard Island and McDonald Islands', 'Austria', 'San Marino', 'Congo', 'Svalbard & Jan Mayen Islands', 'Papua New Guinea', 'Iceland', 'Iceland', 'Bosnia and Herzegovina', "Democratic People's Republic of Korea"]
        assert list(result['data']['counterparty_legal_phone_number'][0:10]) == ['9687 937447', '7772 084705', '9621 880720', '1684 702261', '0847 468066', '4111 801405', '2372 180716', '2372 180716', '0081 009772', '4949 998070']

    def test_convert_counterparty_without_req_file_returns_expected_error(self, s3_client, file_name1="address", file_name2="department"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key1 = f"update_test/{timestamp}/{filename1}.csv"
        key2 = f"update_test/{timestamp}/{filename2}.csv"
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
        result = convert_counterparty(s3_client, session)
        assert result['status'] == "failure"
        assert str(result['message']) == f"No files Found on: s3://{bucket}/update_test/{timestamp}/counterparty.csv."

    def test_convert_counterparty_without_req_bucket_returns_expected_error(self, s3_client, file_name1="address", file_name2="counterparty"):
        timestamp = "2024-05-20 12:10:03.998128"
        filename1 = f"test/data/{file_name1}.csv"
        filename2 = f"test/data/{file_name2}.csv"
        key1 = f"update_test/{timestamp}/{filename1}.csv"
        key2 = f"update_test/{timestamp}/{filename2}.csv"
        bucket = "detergent-tab-ingestion-zone"
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename1, Bucket=bucket, Key=key1)
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        result = convert_counterparty(s3_client, session)
        #error message on utils line 43-47 need improvement?
        assert result['status'] == "failure"
        assert result['timestamp'] == ""

