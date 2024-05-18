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
