from pg8000.native import Connection
from src.load_lambda.credentials_manager import get_secret

creds = get_secret()

# user = creds["warehouse_username"]
# password = creds["warehouse_password"]
# database = creds["warehouse_dbname"]
# host = creds["warehouse_host"]
# port = creds["warehouse_port"]


def connect_to_db():
    return Connection(
        user=user, password=password, database=database, port=port, host=host
    )
