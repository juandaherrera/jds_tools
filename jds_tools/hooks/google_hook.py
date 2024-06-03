import enum
import json
import logging
import os
import sys
from typing import Union

import gspread
import pandas as pd

from .base import BaseHook

logger = logging.getLogger(__name__)


class GoogleCredentialsType(enum.Enum):
    FILE = "file"
    VARIABLE = "variable"


SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    # 'https://www.googleapis.com/auth/drive',
]


class GoogleBaseHook(BaseHook):
    """
    A hook for interacting with Google APIs using service account credentials.

    Args:
        credentials (Union[str, dict]): The path to the credentials file or the credentials as a dictionary.
        credentials_type (GoogleCredentialsType, optional): The type of credentials provided. Defaults to GoogleCredentialsType.FILE.

    Raises:
        ValueError: If the credentials file path is invalid or has an unsupported file format.
        ValueError: If the credentials format is invalid.

    Attributes:
        _gc (gspread.Client): The Google Sheets client object.

    """

    def __init__(
        self,
        credentials: Union[str, dict],
        *,
        credentials_type: GoogleCredentialsType = GoogleCredentialsType.FILE,
    ):
        if credentials_type == GoogleCredentialsType.FILE:
            if os.path.isfile(credentials) and credentials.endswith('.json'):
                self._gc = gspread.service_account(filename=credentials, scopes=SCOPES)
            else:
                raise ValueError("Invalid credentials file path or file format.")
        elif credentials_type == GoogleCredentialsType.VARIABLE:
            try:
                credentials = (
                    json.loads(credentials) if isinstance(credentials, str) else credentials
                )
                self._gc = gspread.service_account_from_dict(credentials, scopes=SCOPES)
            except json.JSONDecodeError:
                raise ValueError("Invalid credentials format.")
        logger.info("GoogleBaseHook initialized Succesfully.")


class GoogleSheetsHook(GoogleBaseHook):
    """
    A hook for interacting with Google Sheets.

    Args:
        spreadsheet_id (str): The ID of the Google Sheets spreadsheet.
        credentials (Union[str, dict]): The credentials to authenticate with Google API.
            It can be either a path to a credentials file or a dictionary containing the credentials.
        credentials_type (GoogleCredentialsType, optional): The type of credentials provided.
            Defaults to GoogleCredentialsType.FILE.

    Attributes:
        spreadsheet_id (str): The ID of the Google Sheets spreadsheet.

    """

    def __init__(
        self,
        spreadsheet_id: str,
        *,
        credentials: Union[str, dict],
        credentials_type: GoogleCredentialsType = GoogleCredentialsType.FILE,
    ):
        super().__init__(credentials, credentials_type=credentials_type)
        self._spreadsheet_id = spreadsheet_id

    @property
    def spreadsheet_id(self) -> str:
        """
        Get the ID of the Google Sheets spreadsheet.

        Returns:
            str: The ID of the Google Sheets spreadsheet.

        """
        return self._spreadsheet_id

    @spreadsheet_id.setter
    def spreadsheet_id(self, spreadsheet_id: str) -> None:
        """
        Set the ID of the Google Sheets spreadsheet.

        Args:
            spreadsheet_id (str): The ID of the Google Sheets spreadsheet.

        """
        self._spreadsheet_id = spreadsheet_id

    @property
    def spreadsheet(self) -> gspread.Spreadsheet:
        """
        Get the Google Sheets spreadsheet object.

        Returns:
            gspread.Spreadsheet: The Google Sheets spreadsheet object.

        """
        return self._gc.open_by_key(self.spreadsheet_id)

    def __adapt_data_for_gsheets(
        self,
        data: Union[list[list[str]], pd.DataFrame],
        include_headers: bool,
        include_index: bool,
    ):
        if isinstance(data, pd.DataFrame):
            new_data = data.to_numpy().tolist()
            if include_headers:
                headers = [list(data.columns)]
                new_data = headers + new_data
            if include_index:
                index = data.index.tolist()
                new_data = [[i] + row for i, row in zip(index, new_data)]
        else:
            new_data = data
        return new_data

    def read(
        self,
        worksheet_name: str,
        table_range: str = None,
        *,
        return_df: bool = False,
        has_headers: bool = True,
    ) -> Union[list[list[str]], pd.DataFrame]:
        """
        Read data from a worksheet in the Google Sheets spreadsheet.

        Args:
            worksheet_name (str): The name of the worksheet.
            table_range (str, optional): The range of cells to read. Defaults to None, which reads all cells.
            return_df (bool, optional): Whether to return the data as a pandas DataFrame. Defaults to False.
            has_headers (bool, optional): Whether the data has headers. Defaults to True.

        Returns:
            Union[list[list[str]], pd.DataFrame]: The read data. If return_df is True, a pandas DataFrame is returned,
            otherwise a list of lists is returned.

        """
        sheet = self.spreadsheet.worksheet(worksheet_name)
        values = sheet.get_all_values(range_name=table_range)
        if return_df:
            headers = values[0] if has_headers else None
            data = values[1:] if has_headers else values
            df = pd.DataFrame(data, columns=headers)
            return df
        else:
            return values

    def write(
        self,
        worksheet_name: str,
        data: Union[list[list[str]], pd.DataFrame],
        *,
        table_range: str = "A1",
        include_index: bool = False,
        include_headers: bool = True,
        raw: bool = False,
    ) -> None:
        """
        Write data to a worksheet in the Google Sheets spreadsheet.

        Args:
            worksheet_name (str): The name of the worksheet.
            data (Union[list[list[str]], pd.DataFrame]): The data to write. It can be either a list of lists or a pandas DataFrame.
            table_range (str, optional): The range of cells to write to. Defaults to "A1".
            include_index (bool, optional): Whether to include the index when writing a pandas DataFrame. Defaults to False.
            include_headers (bool, optional): Whether to include the headers when writing a pandas DataFrame. Defaults to True.
            raw (bool, optional): Whether to write the data as raw values. Defaults to False.

        """
        sheet = self.spreadsheet.worksheet(worksheet_name)
        new_data = self.__adapt_data_for_gsheets(data, include_headers, include_index)
        sheet.update(table_range, new_data, raw=raw)
        logger.info(f"Data written to {worksheet_name} worksheet successfully.")

    def append(
        self,
        worksheet_name: str,
        data: Union[list[str], pd.Series],
        *,
        table_range: str = None,
        include_index: bool = False,
        include_headers: bool = False,
        raw: bool = False,
    ) -> None:
        """
        Append data to a worksheet in the Google Sheets spreadsheet.

        Args:
            worksheet_name (str): The name of the worksheet.
            data (Union[list[str], pd.Series]): The data to append. It can be either a list or a pandas Series.
            include_index (bool, optional): Whether to include the index when appending a pandas Series. Defaults to False.
            include_headers (bool, optional): Whether to include the headers when appending a pandas Series. Defaults to True.
            raw (bool, optional): Whether to append the data as raw values. Defaults to False.

        """
        sheet = self.spreadsheet.worksheet(worksheet_name)
        new_data = self.__adapt_data_for_gsheets(data, include_headers, include_index)
        sheet.append_rows(
            new_data, value_input_option="RAW" if raw else "USER_ENTERED", table_range=table_range
        )
        logger.info(f"Data appended to {worksheet_name} worksheet successfully.")

    def clear(self, worksheet_name: str, table_range: Union[str, list[str]] = None) -> None:
        """
        Clears the data from a specified worksheet in the spreadsheet.

        Args:
            worksheet_name (str): The name of the worksheet to clear.
            table_range (Union[str, list[str]], optional): The range of cells or tables to clear. Defaults to None.

        Returns:
            None
        """
        sheet = self.spreadsheet.worksheet(worksheet_name)
        if table_range is None:
            sheet.clear()
            logger.info(f"All data from {worksheet_name} worksheet cleaned successfully.")
            return
        table_range = [table_range] if isinstance(table_range, str) else table_range
        sheet.batch_clear(table_range)
        logger.info(f"Data cleared from {worksheet_name} worksheet successfully.")
