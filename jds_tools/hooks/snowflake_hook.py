import logging

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from .base import BaseHook

logging.getLogger("Hooks")


class SnowflakeHook(BaseHook):

    def __init__(
        self, account: str, user: str, password: str, database: str, warehouse: str
    ) -> None:
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.warehouse = warehouse
        self.create_engine()

    @property
    def basic_connection_data(self) -> dict:
        return dict(account=self.account, user=self.user, password=self.password)

    def create_engine(self, **kwards) -> None:
        if not kwards:
            self.url = URL(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                warehouse=self.warehouse,
            )
        else:
            self.url = URL(**{**self.basic_connection_data, **kwards})
        self.engine = create_engine(self.url)

    def fetch_data(self, query: str) -> pd.DataFrame:
        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except SQLAlchemyError as e:
            logging.error(f"Error trying to fetch data from Snowflake. Details: {e}")
            return pd.DataFrame()

    def dispose_engine(self) -> None:
        self.engine.dispose()
