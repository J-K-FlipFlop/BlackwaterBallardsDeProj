import pytest
import boto3
import os
import pandas as pd
from moto import mock_aws
from src.load_lambda.utils import (
    get_data_from_processed_zone,
    get_latest_processed_file_list,
    insert_data_into_data_warehouse,
    get_insert_query,
)
from test_warehouse_db import seed_warehouse_db, root_warehouse_db


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestGetLatestProcessedFileList:
    def test_function_returns_failure_message_if_bucket_does_not_exist(
        self, s3_client
    ):
        timestamp = "2024-05-20 12:10:03.998128"
        result = get_latest_processed_file_list(s3_client, timestamp)
        assert result["status"] == "failure"

    def test_function_returns_success_message_when_bucket_exists(
        self, s3_client
    ):
        timestamp = "2024-05-20 12:10:03.998128"
        bucket = "blackwater-processed-zone"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.csv"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_latest_processed_file_list(s3_client, timestamp)
        assert result["status"] == "success"

    def test_function_returns_list_of_recent_files(self, s3_client):
        bucket = "blackwater-processed-zone"
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.parquet"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_latest_processed_file_list(s3_client, timestamp)
        assert result["file_list"] == [
            "2024-05-20 12:10:03.998128/staff.parquet"
        ]

    def test_function_returns_only_files_that_are_in_latest_folder(
        self, s3_client
    ):
        bucket = "blackwater-processed-zone"
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.parquet"
        timestamp2 = "2024-05-18 12:10:03.998128"
        key2 = f"{timestamp2}/staff.parquet"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key2)
        result = get_latest_processed_file_list(s3_client, timestamp)
        assert result["file_list"] == [
            "2024-05-20 12:10:03.998128/staff.parquet"
        ]

    def test_function_returns_multiple_files_that_are_in_latest_folder(
        self, s3_client
    ):
        bucket = "blackwater-processed-zone"
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"{timestamp}/staff.parquet"
        key2 = f"{timestamp}/sales_order.parquet"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key2)
        result = get_latest_processed_file_list(s3_client, timestamp)
        assert result["file_list"] == [
            "2024-05-20 12:10:03.998128/sales_order.parquet",
            "2024-05-20 12:10:03.998128/staff.parquet",
        ]


class TestGetProcessedData:

    def test_output_is_failure_if_no_bucket(self, s3_client):
        result = get_data_from_processed_zone(
            client=s3_client, pq_key="test.parquet"
        )
        assert result["status"] == "failure"

    def test_output_is_dict_containing_pandas_dataframe(self, s3_client):
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        filename = "test/data/parquet/test.parquet"
        bucket = "blackwater-processed-zone"
        key = "test.parquet"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_data_from_processed_zone(
            client=s3_client, pq_key="test.parquet"
        )
        assert isinstance(result, dict)
        assert isinstance(result["data"], pd.DataFrame)

    def test_dataframe_content_matches_file_content(self, s3_client):
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        filename = "test/data/parquet/test.parquet"
        bucket = "blackwater-processed-zone"
        key = "test.parquet"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        result = get_data_from_processed_zone(
            client=s3_client, pq_key="test.parquet"
        )
        assert len(result["data"]) == 2


class TestInsertDataIntoWarehouse:
    def test_table_name_extracted_correctly(self, s3_client):
        conn = root_warehouse_db()
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = "2024-05-20 12:10:03.998128"
        input_key = f"{timestamp}/currency.parquet"
        filename = "test/data/parquet/currency.parquet"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=input_key)
        expected = "currency"
        result = insert_data_into_data_warehouse(
            client=s3_client, pq_key=input_key, connection=conn
        )
        conn.close()
        assert result["table_name"] == expected

    def test_data_is_successfully_written_to_data_warehouse(self, s3_client):
        conn = root_warehouse_db()
        seed_warehouse_db(conn)
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = "2024-05-20 12:10:03.998128"
        input_key = f"{timestamp}/dim_currency.parquet"
        filename = "test/data/parquet/currency.parquet"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=input_key)
        result = insert_data_into_data_warehouse(
            client=s3_client, pq_key=input_key, connection=conn
        )
        conn.close()
        assert result["status"] == "success"


class TestGetQuery:
    def test_query_returns_correct_sql_insert_statement_currency(
        self, s3_client
    ):
        table_name = "dim_currency"
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "test/data/parquet/currency.parquet"
        bucket = "blackwater-processed-zone"
        key = "currency.parquet"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        data = get_data_from_processed_zone(client=s3_client, pq_key=key)[
            "data"
        ]
        result = get_insert_query(table_name, data)
        assert (
            result
            == "INSERT INTO dim_currency VALUES (1, 'GBP', 'Pounds'), (2, 'USD', 'US dollars'), (3, 'EUR', 'Euros') RETURNING *;"
        )

    def test_query_returns_correct_sql_insert_statement_location(
        self, s3_client
    ):
        table_name = "dim_location"
        s3_client.create_bucket(
            Bucket="blackwater-processed-zone",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "test/data/parquet/location.parquet"
        bucket = "blackwater-processed-zone"
        key = "location.parquet"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        data = get_data_from_processed_zone(client=s3_client, pq_key=key)[
            "data"
        ]
        result = get_insert_query(table_name, data)
        assert (
            result
            == "INSERT INTO dim_location VALUES (1, '6826 Herzog Via', null, 'Avon', 'New Patienceburgh', '28441', 'Turkey', '1803 637401'), (2, '179 Alexie Cliffs', null, null, 'Aliso Viejo', '99305-7380', 'San Marino', '9621 880720'), (3, '148 Sincere Fort', null, null, 'Lake Charles', '89360', 'Samoa', '0730 783349'), (4, '6102 Rogahn Skyway', null, 'Bedfordshire', 'Olsonside', '47518', 'Republic of Korea', '1239 706295'), (5, '34177 Upton Track', null, null, 'Fort Shadburgh', '55993-8850', 'Bosnia and Herzegovina', '0081 009772'), (6, '846 Kailey Island', null, null, 'Kendraburgh', '08841', 'Zimbabwe', '0447 798320'), (7, '75653 Ernestine Ways', null, 'Buckinghamshire', 'North Deshaun', '02813', 'Faroe Islands', '1373 796260'), (8, '0579 Durgan Common', null, null, 'Suffolk', '56693-0660', 'United Kingdom', '8935 157571'), (9, '644 Edward Garden', null, 'Borders', 'New Tyra', '30825-5672', 'Australia', '0768 748652'), (10, '49967 Kaylah Flat', 'Tremaine Circles', 'Bedfordshire', 'Beaulahcester', '89470', 'Democratic Peoples Republic of Korea', '4949 998070'), (11, '249 Bernier Mission', null, 'Buckinghamshire', 'Corpus Christi', '85111-9300', 'Japan', '0222 525870'), (12, '6461 Ernesto Expressway', null, 'Berkshire', 'Pricetown', '37167-0340', 'Tajikistan', '4757 757948'), (13, '80828 Arch Dale', 'Torphy Turnpike', null, 'Shanahanview', '60728-5019', 'Bouvet Island (Bouvetoya)', '8806 209655'), (14, '84824 Bryce Common', 'Grady Turnpike', null, 'Maggiofurt', '50899-1522', 'Iraq', '3316 955887'), (15, '605 Haskell Trafficway', 'Axel Freeway', null, 'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands', '9687 937447'), (16, '511 Orin Extension', 'Cielo Radial', 'Buckinghamshire', 'South Wyatt', '04524-5341', 'Iceland', '2372 180716'), (17, '962 Koch Drives', null, null, 'Hackensack', '95316-4738', 'Indonesia', '5507 549583'), (18, '58805 Sibyl Cliff', 'Leuschke Glens', 'Bedfordshire', 'Lake Arne', '63808', 'Kiribati', '0168 407254'), (19, '0283 Cole Corner', 'Izabella Ways', 'Buckinghamshire', 'West Briellecester', '01138', 'Sierra Leone', '1753 158314'), (20, '22073 Klein Landing', null, null, 'Pueblo', '91445', 'Republic of Korea', '4003 678621'), (21, '389 Georgette Ridge', null, 'Cambridgeshire', 'Fresno', '91510-3655', 'Bolivia', '8697 474676'), (22, '364 Goodwin Streets', null, null, 'Sayreville', '85544-4254', 'Svalbard & Jan Mayen Islands', '0847 468066'), (23, '822 Providenci Spring', null, 'Berkshire', 'Derekport', '25541', 'Papua New Guinea', '4111 801405'), (24, '8434 Daren Freeway', null, null, 'New Torrance', '17110', 'Antigua and Barbuda', '5582 055380'), (25, '253 Ullrich Inlet', 'Macey Wall', 'Borders', 'East Arvel', '35397-9952', 'Sudan', '0021 366201'), (26, '522 Pacocha Branch', null, 'Bedfordshire', 'Napa', '77211-4519', 'American Samoa', '5794 359212'), (27, '7212 Breitenberg View', 'Nora Bridge', 'Buckinghamshire', 'Oakland Park', '77499', 'Guam', '2949 665163'), (28, '079 Horacio Landing', null, null, 'Utica', '93045', 'Austria', '7772 084705'), (29, '37736 Heathcote Lock', 'Noemy Pines', null, 'Bartellview', '42400-5199', 'Congo', '1684 702261'), (30, '0336 Ruthe Heights', null, 'Buckinghamshire', 'Lake Myrlfurt', '94545-4284', 'Falkland Islands (Malvinas)', '1083 286132') RETURNING *;"
        )
