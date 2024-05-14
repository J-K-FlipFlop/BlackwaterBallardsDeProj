from src.connection import connect_to_db

def test_query():
    conn = connect_to_db()
    result = conn.run("""SELECT * FROM design limit 5;""")
    conn.close()
    print(result)
    return result

test_query()