import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from jds_tools.hooks.snowflake_hook import SnowflakeHook


@pytest.fixture
def snowflake_hook():
    return SnowflakeHook("account", "user", "password", "database", "warehouse")


@pytest.mark.unit
def test_fetch_data(snowflake_hook):
    test_query = "SELECT * FROM test_table"
    test_data = [
        (1, "Juan"),
        (2, "David"),
    ]
    columns = ["id", "name"]
    expected_df = pd.DataFrame(test_data, columns=columns)

    # Engine and result mock
    engine_mock = MagicMock()
    connection_mock = engine_mock.connect.return_value.__enter__.return_value
    result_mock = connection_mock.execute.return_value
    result_mock.fetchall.return_value = test_data
    result_mock.keys.return_value = columns

    with patch('jds_tools.hooks.snowflake_hook.create_engine', return_value=None):
        snowflake_hook.engine = engine_mock

        result_df = snowflake_hook.fetch_data(test_query)

        pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.unit
def test_split_queries(snowflake_hook):
    test_query = """
        -- This is a comment
        SELECT * FROM table1; -- Query 1
        SELECT * FROM table2; -- Query 2
        -- SELECT * FROM table3; -- Query 3 (commented out)
        SELECT * FROM table4; -- Query 4
    """
    test_query_2 = """
        BEGIN;

        SELECT 
            test_column:'//not_should_be_removed',
            test_column_2 // should be removed 
        FROM my_table;

        COMMIT;
    """
    expected_queries = ["SELECT * FROM table1;", "SELECT * FROM table2;", "SELECT * FROM table4;"]
    expected_queries_2 = [
        "BEGIN;",
        "SELECT test_column:'//not_should_be_removed', test_column_2 FROM my_table;",
        "COMMIT;",
    ]

    result_queries = snowflake_hook._split_queries(test_query)
    result_queries_2 = snowflake_hook._split_queries(test_query_2)
    print(result_queries_2)

    assert result_queries == expected_queries
    assert result_queries_2 == expected_queries_2


@pytest.mark.unit
def test_hidden_password(snowflake_hook):
    assert snowflake_hook.password == "***"
    assert snowflake_hook.url == "snowflake://user:***@account/warehouse?warehouse=database"
