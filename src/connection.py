from pg8000.native import Connection
from credentials_manager import get_secret

user = get_secret()["username"]
password = get_secret()["password"]
database = get_secret()["dbname"]
host = get_secret()["host"]
port = get_secret()["port"]


def connect_to_db():
    return Connection(user=user, password=password, database=database, port=port, host=host)