from pg8000.native import Connection
from src.extract_lambda.credentials_manager import get_secret

creds = get_secret()

user = creds["username"]
password = creds["password"]
database = creds["dbname"]
host = creds["host"]
port = creds["port"]


def connect_to_db() -> Connection:
    """Returns a pg8000 database connection using credentials for the Totesys
    database obtained from AWS SecretsManager via the get_secret function"""

    return Connection(
        user=user, password=password, database=database, port=port, host=host
    )
