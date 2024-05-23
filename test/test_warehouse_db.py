from fastapi import FastAPI
from pprint import pprint as pp
from pg8000.native import Connection

app = FastAPI()

@app.get("/api")
def root_warehouse_db():
    conn = Connection(user=, password=, database="totesys", port=5432, host=,)
    conn.run("DROP DATABASE IF EXISTS totesys;")
    conn.run("CREATE DATABASE totesys;")
    conn.run("""CREATE TABLE
        fact_sales_order(
            sales_record_id SERIAL PRIMARY KEY,
            sales_order_id INT NOT NULL,
            created_date DATE NOT NULL REFERENCES dim_date(date_id),
            created_time TIME NOT NULL,
            last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
            last_updated_time TIME NOT NULL,
            sales_staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
            counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
            units_sold INT NOT NULL,
            unit_price NUMERIC(10,2) NOT NULL,
            currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
            design_id INT NOT NULL REFERENCES dim_design(design_id),
            agreed_payment_date DATE NOT NULL REFERENCES dim_date(date_id),
            agreed_delivery_date DATE NOT NULL REFERENCES dim_date(date_id),
            agreed_delivery_locatiion_id INT NOT NULL REFERENCES dim_location(location_id)
            );""")
    conn.run("""CREATE TABLE
        dim_date(
            date_id DATE PRIMARY KEY NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            day_of_week INT NOT NULL,
            month_name VARCHAR NOT NULL,
            quarter INT NOT NULL
            );""")
    conn.run("""CREATE TABLE
        dim_staff(
            staff_id INT PRIMARY KEY NOT NULL,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            department_name VARCHAR NOT NULL,
            location VARCHAR NOT NULL,
            email_address EMAIL_ADDRESS NOT NULL
            );""")
    conn.run("""CREATE TABLE
        dim_currency(
            currency_id INT PRIMARY KEY NOT NULL,
            currency_code VARCHAR NOT NULL,
            currency_name VARCHAR NOT NULL
            );""")
    conn.run("""CREATE TABLE
        dim_design(
            design_id INT PRIMARY KEY NOT NULL,
            design_name VARCHAR NOT NULL,
            file_location VARCHAR NOT NULL,
            file_name VARCHAR NOT NULL
            );""")
    conn.run("""CREATE TABLE
        dim_location(
            location_id INT PRIMARY KEY NOT NULL,
            address_line_1 VARCHAR NOT NULL,
            address_line_2 VARCHAR,
            district VARCHAR,
            city VARCHAR NOT NULL,
            postal_code VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            phone VARCHAR NOT NULL
            );""")
    conn.run("""CREATE TABLE
        dim_counterparty(
            counterparty_id INT PRIMARY KEY NOT NULL,
            counterparty_legal_name VARCHAR NOT NULL,
            counterparty_legal_address_line_1 varchar VARCHAR NOT NULL,
            counterparty_legal_address_line2 VARCHAR NOT NULL,
            counterparty_legal_district VARCHAR NOT NULL,
            counterparty_legal_city VARCHAR NOT NULL,
            counterparty_legal_postal_code VARCHAR NOT NULL,
            counterparty_legal_country VARCHAR NOT NULL,
            counterparty_legal_phone_number VARCHAR NOT NULL,
            );""")
    #seeding the database in the right way
    