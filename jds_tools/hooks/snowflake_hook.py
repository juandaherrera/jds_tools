import logging

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from .base import DataHook

# Set up the logger
logger = logging.getLogger(__name__)


class SnowflakeHook(DataHook):
    """Handles connections and queries to a Snowflake database.

    This class provides functionality to connect to a Snowflake database, execute queries, and fetch data,
    encapsulating the complexity of handling database operations.

    Attributes:
        account (str): Snowflake account name.
        user (str): Snowflake user name.
        password (str): Password for the Snowflake user.
        database (str): Name of the database to connect to.
        warehouse (str): Name of the warehouse to use.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine instance for database connection.
    """

    def __init__(
        self, account: str, user: str, password: str, database: str, warehouse: str
    ) -> None:
        """Initializes a new SnowflakeHook instance and creates a database engine.

        Args:
            account (str): Snowflake account name.
            user (str): Snowflake user name.
            password (str): Password for the Snowflake user.
            database (str): Name of the database to connect to.
            warehouse (str): Name of the warehouse to use.
        """
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.warehouse = warehouse
        self.engine = None
        self.create_engine()

    def create_engine(self) -> None:
        """Creates a SQLAlchemy engine for the Snowflake database."""
        url = URL(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse,
        )
        self.engine = create_engine(url)

    def fetch_data(self, query: str) -> pd.DataFrame:
        """Executes a SQL query and returns the results as a pandas DataFrame.

        Args:
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame: DataFrame containing the results of the query.

        Raises:
            SQLAlchemyError: An error from SQLAlchemy when executing the query.
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except SQLAlchemyError as e:
            logger.error(f"Error fetching data from Snowflake: {e}")

    def dispose_engine(self) -> None:
        """Disposes of the current SQLAlchemy engine."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
