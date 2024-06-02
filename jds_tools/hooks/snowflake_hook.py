import logging
from typing import List, Literal, Optional, Union

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from .base import DataHook

# Set up the logger
logger = logging.getLogger(__name__)


class SnowflakeHook(DataHook):
    """
    A class representing a Snowflake connection hook.

    This class provides methods to connect to a Snowflake database, execute SQL queries,
    fetch data, and upload data to Snowflake.

    Args:
        account (str): The Snowflake account name.
        user (str): The Snowflake user name.
        password (str): The Snowflake user password.
        warehouse (str): The Snowflake warehouse name.
        database (Optional[str], optional): The Snowflake database name. Defaults to None.
        schema (Optional[str]): The Snowflake schema to use. Defaults to None.
        role (Optional[str], optional): The Snowflake role name. Defaults to None.
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
    ) -> None:
        """
        Initializes a SnowflakeHook object.

        Args:
            account (str): The Snowflake account name.
            user (str): The username for connecting to Snowflake.
            password (str): The password for connecting to Snowflake.
            warehouse (str): The Snowflake warehouse to use.
            database (Optional[str]): The Snowflake database to use. Defaults to None.
            schema (Optional[str]): The Snowflake schema to use. Defaults to None.
            role (Optional[str]): The Snowflake role to use. Defaults to None.
        """
        self._account = account
        self._user = user
        self.__password = password
        self._warehouse = warehouse
        self._database = database
        self._schema = schema
        self._role = role
        self.__url: str = None
        self.__update_engine()

    @property
    def account(self) -> str:
        return self._account

    @account.setter
    def account(self, value: str) -> None:
        self._account = value
        self.__update_engine()

    @property
    def user(self) -> str:
        return self._user

    @user.setter
    def user(self, value: str) -> None:
        self._user = value
        self.__update_engine()

    @property
    def password(self) -> str:
        return "***"

    @password.setter
    def password(self, value: str) -> None:
        self.__password = value
        self.__update_engine()

    @property
    def warehouse(self) -> str:
        return self._warehouse

    @warehouse.setter
    def warehouse(self, value: str) -> None:
        self._warehouse = value
        self.__update_engine()

    @property
    def database(self) -> Optional[str]:
        return self._database

    @database.setter
    def database(self, value: Optional[str]) -> None:
        self._database = value
        self.__update_engine()

    @property
    def schema(self) -> Optional[str]:
        return self._schema

    @schema.setter
    def schema(self, value: Optional[str]) -> None:
        self._schema = value
        self.__update_engine()

    @property
    def role(self) -> Optional[str]:
        return self._role

    @role.setter
    def role(self, value: Optional[str]) -> None:
        self._role = value
        self.__update_engine()

    @property
    def url(self) -> str:
        return self.__url.replace(f"{self.user}:{self.__password}", f"{self.user}:{self.password}")

    @property
    def connection_data(self) -> dict:
        """
        Returns a dictionary containing the connection data for Snowflake.

        Returns:
            dict: A dictionary containing the connection data.
                The keys are 'account', 'user', 'password', 'warehouse',
                'database' (optional), and 'role' (optional).
        """
        return {
            key: value
            for key, value in {
                "account": self.account,
                "user": self.user,
                "password": self.__password,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
                "role": self.role,
            }.items()
            if value is not None
        }

    def __str__(self) -> str:
        """
        Returns a string representation of the SnowflakeHook object with the password masked.

        Returns:
            str: A string representation of the object with the password masked.
        """
        parts = [
            f"SnowflakeHook(account='{self.account}', user='{self.user}', password='{self.password}', warehouse='{self.warehouse}'"
        ]
        if self.database:
            parts.append(f", database='{self.database}'")
        if self.schema:
            parts.append(f", schema='{self.schema}'")
        if self.role:
            parts.append(f", role='{self.role}'")
        parts.append(")")
        return "".join(parts)

    def __update_engine(self, **kwards) -> None:
        """
        Update the Snowflake connection engine.

        This method updates the connection data and creates a new SQLAlchemy engine object
        for the Snowflake connection.

        Args:
            **kwards: Optional keyword arguments to update the connection data.

        """
        logging.info("Updating Snowflake url and engine.")
        self.dispose_engine()
        self.__url = URL(**self.connection_data)
        self.engine = create_engine(self.__url)

    def execute_statement(self, query: str) -> None:
        """
        Executes the given SQL query or queries on the Snowflake database.

        Args:
            query (str): The SQL query or queries to execute. Multiple queries should be separated by ';'.

        Raises:
            SQLAlchemyError: If there is an error executing the query.

        Returns:
            None
        """
        try:
            individual_queries = self._split_queries(query)
            with self.engine.connect() as connection:
                for q in individual_queries:
                    connection.execute(q)
            logging.info("Query executed successfully on Snowflake.")
        except SQLAlchemyError as e:
            logging.error(f"Error trying to execute query on Snowflake. Details: {e}")

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
        schema: str = None,
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
        schema = schema or self.schema or ""
        try:
            data.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists_method,
                index=False,
                chunksize=chunk_size,
            )
            logging.info(f"Data uploaded to Snowflake ({self.database}.{schema}.{table_name}).")
        except Exception as e:
            logging.error(
                f"Error trying to upload data to Snowflake ({self.database}.{schema}.{table_name}). Details: {e}"
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
