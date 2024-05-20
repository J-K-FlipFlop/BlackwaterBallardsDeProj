from pg8000.native import Connection
from src.extract_lambda.credentials_manager import get_secret

creds = get_secret()

user = creds["username"]
password = creds["password"]
database = creds["dbname"]
host = creds["host"]
port = creds["port"]


def connect_to_db():
    """Establishes a pg8000 connection to the Totesys database, using the
    credentials stored in the AWS Secret Manager"""
    return Connection(
        user=user, password=password, database=database, port=port, host=host
    )
