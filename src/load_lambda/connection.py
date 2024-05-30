from pg8000.native import Connection
from src.load_lambda.credentials_manager import get_secret

creds = get_secret()

user = creds["username"]
password = creds["password"]
database = creds["dbname"]
host = creds["host"]
port = creds["port"]
schema = creds["schema"]


def connect_to_db() -> Connection:
    """Returns a pg8000 database connection using credentials for the Data
    Warehouse obtained from AWS SecretsManager via the get_secret function"""

    return Connection(
        user=user, password=password, database=database, port=port, host=host
    )
