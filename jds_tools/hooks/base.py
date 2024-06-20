import re
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
        excluding lines that are commented out with "--" or "//" before the semicolon, and considering
        that "--" and "//" within single or double quotes should not be treated as comments.

        Args:
            query (str): The SQL query to split.

        Returns:
            List[str]: A list of individual queries.

        """

        def remove_comments(sql: str) -> str:
            pattern = r"""
            (['"])(?:(?=(\\?))\2.)*?\1    # match strings
            |--.*?$                       # match single line comments
            |//.*?$                       # match single line comments
            """
            regex = re.compile(pattern, re.VERBOSE | re.MULTILINE)

            def _replacer(match: re.Match) -> str:
                if match.group(2) is None:  # Only replace comments
                    return ''
                return match.group(0)

            return regex.sub(_replacer, sql)

        def split_and_clean(sql: str) -> List[str]:
            parts = sql.split(";")
            cleaned_parts = []
            for part in parts:
                cleaned_part = ' '.join(part.split())  # Remove extra spaces
                if cleaned_part:
                    cleaned_parts.append(cleaned_part + ";")
            return cleaned_parts

        # Remove comments
        cleaned_query = remove_comments(query)

        # Split by semicolon and clean up each part
        queries = split_and_clean(cleaned_query)

        return queries
