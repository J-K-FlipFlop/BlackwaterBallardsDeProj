import pytest
from src.utils.dict_formatter import format_to_dict

def test_dict_formatter_returns_dict_type():
    test_func = format_to_dict
    query1 = []
    query2 = []
    assert isinstance(test_func(query1, query2), dict) 
    
def test_dict_returns_expected_format():
    test_func = format_to_dict
    query1 = ['col1']
    query2 = ['res1','res2', 'res3']
    assert test_func(query1, query2) == {'col1': ['res1', 'res2', 'res3']}