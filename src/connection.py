from pg8000.native import Connection
from credentials_manager import get_secret

user = get_secret()["username"]
password = get_secret()["password"]
database = get_secret()["dbname"]
host = get_secret()["host"]
port = get_secret()["port"]


def connect_to_db():
    return Connection(user='project_team_2', password='laWBU6s7imO0XXBC', database='totesys', host='nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com', port=5432)