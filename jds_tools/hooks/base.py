from abc import ABC, abstractmethod

from pandas import DataFrame


class BaseHook(ABC):
    pass


class DataHook(BaseHook):

    @abstractmethod
    def fetch_data(self, query: str) -> DataFrame:
        pass
