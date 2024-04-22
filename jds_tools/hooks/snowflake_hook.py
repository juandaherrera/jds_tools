import logging
from typing import Literal, Union

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from .base import DataHook

# Set up the logger
logger = logging.getLogger(__name__)


class SnowflakeHook(DataHook):
    """
    A class representing a Snowflake database hook.

    This hook provides methods to interact with a Snowflake database, including fetching data, uploading data,
    and managing the database connection.

    Args:
        account (str): The Snowflake account name.
        user (str): The username for authentication.
        password (str): The password for authentication.
        database (str): The name of the Snowflake database.
        warehouse (str): The name of the Snowflake warehouse.

    Attributes:
        account (str): The Snowflake account name.
        user (str): The username for authentication.
        password (str): The password for authentication.
        database (str): The name of the Snowflake database.
        warehouse (str): The name of the Snowflake warehouse.
        _connection_data (dict): The connection data dictionary.
        connection_data (dict): The connection data dictionary.
        engine: The SQLAlchemy engine object for the Snowflake connection.

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
        self._connection_data: dict = None
        self.connection_data = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "warehouse": self.warehouse,
        }
        self.update_engine()

    @property
    def connection_data(self) -> dict:
        return self._connection_data

    @connection_data.setter
    def connection_data(self, value: dict) -> None:
        try:
            self._connection_data = self._connection_data | value
        except:
            self._connection_data = value

    def update_engine(self, **kwards) -> None:
        """
        Update the Snowflake connection engine.

        This method updates the connection data and creates a new SQLAlchemy engine object
        for the Snowflake connection.

        Args:
            **kwards: Optional keyword arguments to update the connection data.

        """
        logging.info("Updating Snowflake url and engine.")
        self.dispose_engine()
        self.connection_data = kwards if kwards else self.connection_data
        self.url = URL(**self.connection_data)
        self.engine = create_engine(self.url)

    def fetch_data(self, query: str, data_return: bool = True) -> Union[pd.DataFrame, None]:
        """
        Fetch data from Snowflake.

        This method executes the given SQL query on the Snowflake connection and returns
        the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.
            data_return (bool, optional): Whether to return the fetched data as a DataFrame.
                Defaults to True.

        Returns:
            Union[pd.DataFrame, None]: The fetched data as a pandas DataFrame, or None if
            `data_return` is set to False.

        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
            logging.info("Data fetched from Snowflake.")
            return pd.DataFrame(result.fetchall(), columns=result.keys()) if data_return else None
        except SQLAlchemyError as e:
            logging.error(f"Error trying to fetch data from Snowflake. Details: {e}")
            return pd.DataFrame() if data_return else None

    def upload_data(
        self,
        data: pd.DataFrame,
        table_name: str,
        if_exists_method: Literal["fail", "replace", "append"] = "append",
        chunk_size: int = 7500,
    ):
        """
        Upload data to Snowflake.

        This method uploads the given pandas DataFrame to the specified table in Snowflake.

        Args:
            data (pd.DataFrame): The data to upload as a pandas DataFrame.
            table_name (str): The name of the table to upload the data to.
            if_exists_method (Literal["fail", "replace", "append"], optional): The method to handle
                the case when the table already exists. Defaults to "append".
            chunk_size (int, optional): The number of rows to insert in each batch. Defaults to 7500.

        """
        try:
            data.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists_method,
                index=False,
                chunksize=chunk_size,
            )
            logging.info(
                "Data uploaded to Snowflake "
                f"({self.database}.{self.connection_data.get('schema', '')}.{table_name})."
            )
        except Exception as e:
            logging.error(
                f"Error trying to upload data to Snowflake ({table_name=}). Details: {e}"
            )

    def dispose_engine(self) -> None:
        """
        Dispose the Snowflake connection engine.

        This method disposes the SQLAlchemy engine object for the Snowflake connection.

        """
        try:
            self.engine.dispose()
        except:
            pass
