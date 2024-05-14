from src.connection import connect_to_db
from pprint import pprint as pp

def test_query():
    conn = connect_to_db()
    result = conn.run("""SELECT * FROM design limit 5;""")
    conn.close()
    pp(result)
    return result

test_query()