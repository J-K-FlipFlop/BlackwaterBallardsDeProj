from pg8000.native import Connection
from src.credentials_manager import get_secret

creds = get_secret()

user = creds["username"]
password = creds["password"]
database = creds["dbname"]
host = creds["host"]
port = creds["port"]


def connect_to_db():
    return Connection(user=user, password=password, database=database, port=port, host=host)