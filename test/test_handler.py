import pytest
from src.handler import connect_to_db_table
from pg8000.exceptions import DatabaseError


def test_function_returns_a_list_of_dicts():
    table = "sales_order"
    result = connect_to_db_table(table)
    assert isinstance(result, list)
    for r in result:
        assert isinstance(r, dict)


def test_header_count_equal_to_result_count():
    table = "staff"
    col_headers = [
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    ]
    result = connect_to_db_table(table)
    assert len(result[0]) == len(col_headers)


def test_returns_error_message_if_table_name_not_found():
    table = "dog"
    result = connect_to_db_table(table)
    assert result == {
        "status": "Failed",
        "message": 'relation "dog" does not exist',
    }


# def test_sql_statement_not_vulnerable_to_injection():
#     table = "staff; drop table staff;"
#     with pytest.raises(DatabaseError):
#         connect_to_db_table(table)
