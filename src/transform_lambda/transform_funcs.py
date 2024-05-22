import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound
import pandas as pd
from src.transform_lambda.utils import read_latest_changes, get_data_from_ingestion_bucket

def convert_design(client, session):
    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        return response1
    
    filename = "design.csv"
    response2 = get_data_from_ingestion_bucket(key, filename, session)

    if response2["status"] == "success":
        df_design = response2["data"]
        df_design = df_design.drop(['created_at', 'last_updated'], axis=1)
    else:
        return response2
    
    print(df_design)
    output = {"status": "success", "data": df_design}
    return output
    
def convert_currency(client, session):
    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        return response1
    
    filename = "currency.csv"
    response2 = get_data_from_ingestion_bucket(key, filename, session)

    if response2["status"] == "success":
        df = response2["data"]
        df = df.drop(['created_at', 'last_updated'], axis=1)
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
    print(df_currency)
    output = {"status": "success", "data": df_currency}
    return output

def convert_staff(client, session):
    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        return response1
    
    filename1 = "staff.csv"
    filename2 = "department.csv"
    response_staff = get_data_from_ingestion_bucket(key, filename1, session)
    response_department = get_data_from_ingestion_bucket(key, filename2, session)

    if response_staff["status"] == "success":
        df_staff = response_staff["data"]
        df_staff = df_staff.drop(['created_at', 'last_updated'], axis=1)
        staff_table_as_dict = df_staff.to_dict()
    else:
        return response_staff
    
    if response_department["status"] == "success":
        df_dep = response_department["data"]
        df_dep = df_dep.drop(["manager",
                              "created_at",
                              "last_updated"],
                               axis=1)
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
    df_staff = df_staff[["staff_id",
                          "first_name",
                          "last_name",
                          d_n,
                          loc,
                          "email_address"]]
    
    print(df_staff)
    output = {"status": "success", "data": df_staff}
    return output

def convert_location(client, session):
    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        return response1
    
    filename = "address.csv"
    response2 = get_data_from_ingestion_bucket(key, filename, session)

    if response2["status"] == "success":
        df = response2["data"]
        df = df.drop(['created_at', 'last_updated'], axis=1)
        table_as_dict = df.to_dict()
    else:
        return response2["message"]
    
    table_as_dict['location_id'] = table_as_dict['address_id']
    df_location = pd.DataFrame(table_as_dict)
    df_location = df_location.drop(['address_id'], axis=1)
    df_location = df_location[['location_id'] + [col for col in df_location.columns if col != 'location_id']]
    print(df_location)
    output = {"status": "success", "data": df_location}
    return output

def convert_counterparty(client, session):
    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        return response1
    filename_address = "address.csv"
    filename_counter = "counterparty.csv"
    response_address = get_data_from_ingestion_bucket(key, filename_address, session)
    response_counter = get_data_from_ingestion_bucket(key, filename_counter, session)

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

    location_ids = dict_counter['legal_address_id']
    dict_counter["counterparty_legal_address_line_1"] = {}
    dict_counter["counterparty_legal_address_line_2"] = {}
    dict_counter["counterparty_legal_district"] = {}
    dict_counter["counterparty_legal_city"] = {}
    dict_counter["counterparty_legal_postal_code"] = {}
    dict_counter["counterparty_legal_country"] = {}
    dict_counter["counterparty_legal_phone_number"] = {}

    for key in location_ids:
        # print(key, '<--- KEY!!')
        ids = location_ids[key]              
        dict_counter["counterparty_legal_address_line_1"][key] = dict_address[
            "address_line_1"
        ][ids -1]
        dict_counter["counterparty_legal_address_line_2"][key] = dict_address[
            "address_line_2"
        ][ids -1]
        dict_counter["counterparty_legal_district"][key] = dict_address[
            "district"
        ][ids -1]
        dict_counter["counterparty_legal_city"][key] = dict_address[
            "city"
        ][ids -1]
        dict_counter["counterparty_legal_postal_code"][key] = dict_address[
            "postal_code"
        ][ids -1]
        dict_counter["counterparty_legal_country"][key] = dict_address[
            "country"
        ][ids -1]
        dict_counter["counterparty_legal_phone_number"][key] = dict_address[
            "phone"
        ][ids -1]
    # pp(dict_counter)
    print(dict_counter["counterparty_legal_address_line_1"], '<--- look here')
    df_counter = pd.DataFrame(dict_counter)
    df_counter = df_counter.drop(
        [
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ], axis=1
    )
    print(df_counter, "<-- df_counter")

def convert_sales_order(client, session):
    response1 = read_latest_changes(client)
    if response1["status"] == "success":
        key = response1["timestamp"]
    else:
        return response1
    filename_sales = 'sales_order.csv'
    response_sales = get_data_from_ingestion_bucket(key, filename_sales, session)

    if response_sales['status'] == 'success':
        df_sales = response_sales['data']
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
    print(df_sales)
    output = {"status": "success", "data": df_sales}
    return output


## This function takes the output of convert_sales_order like so...
## x = convert_sales_order
## df_sales = x["data"]
def create_dim_date(df_sales):
    sales_dict = df_sales.to_dict()
    # print(sales_dict)

session = boto3.session.Session()
client = boto3.client("s3")

convert_design(client, session)
convert_currency(client, session)
convert_staff(client, session)
convert_location(client, session)
convert_counterparty(client, session)
x = convert_sales_order(client, session)
df_sales = x["data"]
create_dim_date(df_sales)