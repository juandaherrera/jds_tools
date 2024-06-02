from abc import ABC, abstractmethod
from typing import List

from pandas import DataFrame


class BaseHook(ABC):
    pass


class DataHook(BaseHook):

    @abstractmethod
    def fetch_data(self, query: str) -> DataFrame:
        pass
    
    @staticmethod
    def _split_queries(query: str) -> List[str]:
        """
        Split a SQL query into individual queries.

        This method splits a SQL query into individual queries based on the semicolon (;) delimiter,
        excluding lines that are commented out with "--" or "//" before the semicolon.

        Args:
            query (str): The SQL query to split.

        Returns:
            List[str]: A list of individual queries.

        """
        lines = query.split('\n')
        queries = []
        current_query = ""
        for line in lines:
            line = line.strip()
            if "--" in line:
                line = line.split("--")[0].strip()
            elif "//" in line:
                line = line.split("//")[0].strip()
            if ";" in line:
                parts = line.split(";")
                for part in parts:
                    if part == "":
                        continue
                    current_query += part + ";"
                    queries.append(current_query.strip())
                    current_query = ""
            else:
                current_query += line + " " if line != "" else ""
        if current_query:
            queries.append(current_query.strip())
        return queries