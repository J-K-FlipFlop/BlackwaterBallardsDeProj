import boto3
import pandas as pd
from src.transform_lambda.utils import (
    read_latest_changes,
    get_data_from_ingestion_bucket,
)


def convert_design(client: boto3.client, session: boto3.session) -> dict:
    """Downloads data from S3 ingestion bucket and transforms it into a pandas
    dataframe, relating to the design table

    Args:
        client: Boto3 client
        session: Boto3 session

    Returns:
        A dictionary containing the following:
            Success message
            data: a pandas dataframe containing transformed data
            (if successful)
        If unsuccessful the dictionaries containing failure messages from the
        util functions are returned
    """

    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        print("file not found")
        return response1

    filename = "design.csv"
    response2 = get_data_from_ingestion_bucket(key, filename, session)

    if response2["status"] == "success":
        df_design = response2["data"]
        df_design = df_design.drop(["created_at", "last_updated"], axis=1)
        df_design = df_design.drop_duplicates()
    else:
        return response2

    output = {"status": "success", "data": df_design}
    return output


def convert_currency(client: boto3.client, session: boto3.session) -> dict:
    """Downloads data from S3 ingestion bucket and transforms it into a pandas
    dataframe, relating to the currency table

    Args:
        client: Boto3 client
        session: Boto3 session

    Returns:
        A dictionary containing the following:
            Success message
            data: a pandas dataframe containing transformed data
            (if successful)
        If unsuccessful the dictionaries containing failure messages from the
        util functions are returned
    """

    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        print("file not found")
        return response1

    filename = "currency.csv"
    response2 = get_data_from_ingestion_bucket(key, filename, session)

    if response2["status"] == "success":
        df = response2["data"]
        df = df.drop(["created_at", "last_updated"], axis=1)
        table_as_dict = df.to_dict()
    else:
        return response2

    curr_codes = table_as_dict["currency_code"]
    curr_names = {}

    for index in curr_codes:
        val = curr_codes[index]
        match val:
            case "GBP":
                curr_names[index] = "Pounds"
            case "USD":
                curr_names[index] = "US dollars"
            case "EUR":
                curr_names[index] = "Euros"
            case _:
                curr_codes[index] = "unknown currency"
    table_as_dict["currency_name"] = curr_names
    df_currency = pd.DataFrame(table_as_dict)
    output = {"status": "success", "data": df_currency}
    return output


def convert_staff(client: boto3.client, session: boto3.session) -> dict:
    """Downloads data from S3 ingestion bucket and transforms it into a pandas
    dataframe, relating to the staff table

    Args:
        client: Boto3 client
        session: Boto3 session

    Returns:
        A dictionary containing the following:
            Success message
            data: a pandas dataframe containing transformed data
            (if successful)
        If unsuccessful the dictionaries containing failure messages from the
        util functions are returned
    """

    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        print("file not found")
        return response1

    filename1 = "staff.csv"
    filename2 = "department.csv"

    response_staff = get_data_from_ingestion_bucket(key, filename1, session)
    response_department = get_data_from_ingestion_bucket(
        key, filename2, session, update=False
    )

    if response_staff["status"] == "success":
        df_staff = response_staff["data"]
        df_staff = df_staff.drop(["created_at", "last_updated"], axis=1)
        staff_table_as_dict = df_staff.to_dict()
    else:
        return response_staff

    if response_department["status"] == "success":
        df_dep = response_department["data"]
        df_dep = df_dep.drop(["manager", "created_at", "last_updated"], axis=1)
        dep_table_as_dict = df_dep.to_dict()
    else:
        return response_department

    dep_id_dict = staff_table_as_dict["department_id"]

    loc = "location"
    d_n = "department_name"

    staff_table_as_dict[loc] = {}
    staff_table_as_dict[d_n] = {}

    for key in dep_id_dict:
        id = dep_id_dict[key]
        staff_table_as_dict[d_n][key] = dep_table_as_dict[d_n][id - 1]
        staff_table_as_dict[loc][key] = dep_table_as_dict[loc][id - 1]

    df_staff = pd.DataFrame(staff_table_as_dict)
    df_staff = df_staff[
        ["staff_id", "first_name", "last_name", d_n, loc, "email_address"]
    ]

    df_staff.replace("'", "", regex=True, inplace=True)
    output = {"status": "success", "data": df_staff}
    return output


def convert_location(
    client: boto3.client, session: boto3.session, update: bool = False
):
    """Downloads data from S3 ingestion bucket and transforms it into a pandas
    dataframe, relating to the location table

    Args:
        client: Boto3 client
        session: Boto3 session
        update: optional, defaults to False

    Returns:
        A dictionary containing the following:
            Success message
            data: a pandas dataframe containing transformed data
            (if successful)
        If unsuccessful the dictionaries containing failure messages from the
        util functions are returned
    """

    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        return response1

    filename = "address.csv"
    response2 = get_data_from_ingestion_bucket(key, filename, session)

    if response2["status"] == "success":
        df = response2["data"]
        df = df.drop(["created_at", "last_updated"], axis=1)
        table_as_dict = df.to_dict()
    else:
        return response2

    table_as_dict["location_id"] = table_as_dict["address_id"]
    df_location = pd.DataFrame(table_as_dict)
    df_location = df_location.drop(["address_id"], axis=1)
    df_location = df_location[
        ["location_id"]
        + [col for col in df_location.columns if col != "location_id"]
    ]
    df_location.replace("'", "", regex=True, inplace=True)
    output = {"status": "success", "data": df_location}
    return output


def convert_counterparty(client: boto3.client, session: boto3.session) -> dict:
    """Downloads data from S3 ingestion bucket and transforms it into a pandas
    dataframe, relating to the counterparty table

    Args:
        client: Boto3 client
        session: Boto3 session

    Returns:
        A dictionary containing the following:
            Success message
            data: a pandas dataframe containing transformed data
            (if successful)
        If unsuccessful the dictionaries containing failure messages from the
        util functions are returned
    """

    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        print("file not found")
        return response1
    filename_address = "address.csv"
    filename_counter = "counterparty.csv"

    response_address = get_data_from_ingestion_bucket(
        key, filename_address, session, update=False
    )
    response_counter = get_data_from_ingestion_bucket(
        key, filename_counter, session
    )

    if response_counter["status"] == "success":
        df_counter = response_counter["data"]
        dict_counter = df_counter.to_dict()
    else:
        return response_counter

    if response_address["status"] == "success":
        df_address = response_address["data"]
        dict_address = df_address.to_dict()
    else:
        return response_address

    location_ids = dict_counter["legal_address_id"]
    dict_counter["counterparty_legal_address_line_1"] = {}
    dict_counter["counterparty_legal_address_line_2"] = {}
    dict_counter["counterparty_legal_district"] = {}
    dict_counter["counterparty_legal_city"] = {}
    dict_counter["counterparty_legal_postal_code"] = {}
    dict_counter["counterparty_legal_country"] = {}
    dict_counter["counterparty_legal_phone_number"] = {}

    for key in location_ids:
        ids = location_ids[key]
        dict_counter["counterparty_legal_address_line_1"][key] = dict_address[
            "address_line_1"
        ][ids - 1]
        dict_counter["counterparty_legal_address_line_2"][key] = dict_address[
            "address_line_2"
        ][ids - 1]
        dict_counter["counterparty_legal_district"][key] = dict_address[
            "district"
        ][ids - 1]
        dict_counter["counterparty_legal_city"][key] = dict_address["city"][
            ids - 1
        ]
        dict_counter["counterparty_legal_postal_code"][key] = dict_address[
            "postal_code"
        ][ids - 1]
        dict_counter["counterparty_legal_country"][key] = dict_address[
            "country"
        ][ids - 1]
        dict_counter["counterparty_legal_phone_number"][key] = dict_address[
            "phone"
        ][ids - 1]
    df_counter = pd.DataFrame(dict_counter)
    df_counter = df_counter.drop(
        [
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ],
        axis=1,
    )
    output = {"status": "success", "data": df_counter}
    return output


def convert_sales_order(client: boto3.client, session: boto3.session) -> dict:
    """Downloads data from S3 ingestion bucket and transforms it into a pandas
    dataframe, relating to the sales_order table

    Args:
        client: Boto3 client
        session: Boto3 session

    Returns:
        A dictionary containing the following:
            Success message
            data: a pandas dataframe containing transformed data
            (if successful)
        If unsuccessful the dictionaries containing failure messages from the
        util functions are returned
    """
    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        print("file not found")
        return response1
    filename_sales = "sales_order.csv"
    response_sales = get_data_from_ingestion_bucket(
        key, filename_sales, session
    )

    if response_sales["status"] == "success":
        df_sales = response_sales["data"]
        sales_dict = df_sales.to_dict()
    else:
        return response_sales

    created_date = {}
    created_time = {}

    for key in sales_dict["created_at"]:
        timestamp = sales_dict["created_at"][key]
        splitted = timestamp.split()
        date = splitted[0]
        time = splitted[1]
        created_date[key] = date
        created_time[key] = time

    last_updated_date = {}
    last_updated_time = {}
    for key in sales_dict["last_updated"]:
        timestamp = sales_dict["last_updated"][key]
        splitted = timestamp.split()
        date = splitted[0]
        time = splitted[1]
        last_updated_date[key] = date
        last_updated_time[key] = time

    sales_dict["created_date"] = created_date
    sales_dict["created_time"] = created_time
    sales_dict["last_updated_date"] = last_updated_date
    sales_dict["last_updated_time"] = last_updated_time

    df_sales = pd.DataFrame(sales_dict)
    df_sales = df_sales.drop(["created_at", "last_updated"], axis=1)
    df_sales = df_sales.rename(columns={"staff_id": "sales_staff_id"})
    df_sales = df_sales.loc[
        :,
        [
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ],
    ]

    output = {"status": "success", "data": df_sales}
    return output


def create_dim_dates(
    client: boto3.client, start: str = "2020-01-01", end: str = "2030-01-01"
):
    """Downloads data from S3 ingestion bucket and transforms it into a pandas
    dataframe, relating to the design table

    Args:
        client: Boto3 client
        start: optional, string containing start date for dim_date table
        end: optional, string containing end date for dim_date table

    Returns:
        A dictionary containing the following:
            Success/Failure message
            data: a pandas dataframe containing transformed data
            (if successful)
            message: an error message (if unsucessful)
    """

    response = read_latest_changes(client)
    if response["timestamp"] != "original_data_dump":
        output = {"status": "failure", "message": "dim date already set"}
        return output
    try:
        df = pd.DataFrame({"date_id": pd.date_range(start, end)})
        df["year"] = df.date_id.dt.year
        df["month"] = df.date_id.dt.month
        df["day"] = df.date_id.dt.day
        df["day_of_week"] = df.date_id.dt.day_of_week + 1
        df["day_name"] = df.date_id.dt.day_name()
        df["month_name"] = df.date_id.dt.month_name()
        df["quarter"] = df.date_id.dt.quarter
        df["date_id"] = pd.to_datetime(df["date_id"]).dt.date
        df["date_id"] = df["date_id"].astype(str)
        df["day_name"] = df["day_name"].astype(str)
        df["month_name"] = df["month_name"].astype(str)
        output = {"status": "success", "data": df}
    except Exception:
        output = {
            "status": "failed",
            "message": "something has gone horrifically wrong, check this",
        }
    return output
