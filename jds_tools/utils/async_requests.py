import asyncio
import json
from typing import Dict, List, Union

import aiohttp
from aiohttp.client_exceptions import ContentTypeError


async def async_get(url_list: List[str], headers_list: List[Dict[str, str]] = None):
    """
    Asynchronously sends GET requests to multiple URLs.

    Args:
        url_list (list[str]): A list of URLs to send GET requests to.
        headers_list (list[dict[str, str]], optional): A list of headers to include in the requests.
            Each header is a dictionary with key-value pairs. Defaults to None.

    Raises:
        TypeError: If url_list is not a list or headers_list is not a list of dictionaries.

    Returns:
        A list of response texts corresponding to each URL in url_list.
    """
    if not isinstance(url_list, list) or (
        headers_list and not all(isinstance(i, dict) for i in headers_list)
    ):
        raise TypeError("url_list must be a list and headers_list must be a list of dictionaries")

    headers_list = headers_list or [{}] * len(url_list)

    async def get_one(url: str, headers: Union[Dict[str, str], None]):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                try:
                    json_content = await response.json()
                except ContentTypeError:
                    json_content = None
                return {
                    "status": response.status,
                    "headers": dict(response.headers),
                    "text": await response.text(),
                    "json": json_content,
                }

    tasks = [get_one(url, headers) for url, headers in zip(url_list, headers_list)]
    return await asyncio.gather(*tasks)


async def async_post(
    url: str, data_list: List[Union[str, Dict]] = None, headers_list: List[Dict[str, str]] = None
):
    """
    Asynchronously sends POST requests to the specified URL with the given data and headers.

    Args:
        url (str): The URL to send the POST requests to.
        data_list (list[str], optional): A list of JSON strings representing the data to send in each request. Defaults to None.
        headers_list (list[dict[str, str]], optional): A list of dictionaries representing the headers to include in each request. Defaults to None.

    Raises:
        TypeError: If data_list or headers_list is not a list, or if headers_list contains elements that are not dictionaries.
        ValueError: If any element in data_list is not a valid JSON string.

    Returns:
        List[str]: A list of response texts from each request, in the same order as the input data_list and headers_list.
    """
    if (
        not isinstance(data_list, list)
        or not isinstance(headers_list, list)
        or not all(isinstance(i, dict) for i in headers_list)
    ):
        raise TypeError("data_list and headers_list must be lists of dictionaries")

    for i, data in enumerate(data_list):
        if isinstance(data, dict):
            data_list[i] = json.dumps(data)
        elif isinstance(data, str):
            try:
                json.loads(data)
            except json.JSONDecodeError:
                raise ValueError("All string elements in data_list must be valid JSON strings")
        else:
            raise TypeError(
                "All elements in data_list must be either dictionaries or valid JSON strings"
            )

    async def post_one(data: str, headers: dict):
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, headers=headers) as response:
                try:
                    json_content = await response.json()
                except ContentTypeError:
                    json_content = None
                return {
                    "status": response.status,
                    "headers": dict(response.headers),
                    "text": await response.text(),
                    "json": json_content,
                }

    tasks = [post_one(data, headers) for data, headers in zip(data_list, headers_list)]
    return await asyncio.gather(*tasks)
