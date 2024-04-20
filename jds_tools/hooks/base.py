from abc import ABC, abstractmethod

from pandas import DataFrame


class BaseHook:

    @abstractmethod
    def fetch_data(self, query: str) -> DataFrame:
        pass
