import pytest

from jds_tools.hooks.base import DataHook


@pytest.mark.unit
def test_split_queries():
    test_query = """
        -- This is a comment
        SELECT * FROM table1; -- Query 1
        SELECT * FROM table2; -- Query 2
        -- SELECT * FROM table3; -- Query 3 (commented out)
        SELECT * FROM table4; -- Query 4
    """
    expected_queries = ["SELECT * FROM table1;", "SELECT * FROM table2;", "SELECT * FROM table4;"]

    result_queries = DataHook._split_queries(test_query)

    assert result_queries == expected_queries
