import asyncio
import json
from typing import Any, Dict, List, Union

import aiohttp
from aiohttp.client_exceptions import ContentTypeError


async def async_get(
    urls: List[str], headers_list: Union[List[Dict[str, str]], Dict[str, str]] = None
) -> List[Dict[str, Any]]:
    """
    Asynchronously sends GET requests to multiple URLs and returns the responses.

    Args:
        urls (List[str]): A list of URLs to send GET requests to.
        headers_list (Union[List[Dict[str, str]], Dict[str, str]], optional): Headers to include in the requests.
            It can be a list of dictionaries, where each dictionary contains headers for a specific URL,
            or a single dictionary that will be used for all URLs. Defaults to None.

    Raises:
        TypeError: If `urls` is not a list or `headers` is not a list of dictionaries or a dictionary.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the responses for each URL.
            Each dictionary contains the following keys:
            - "status": The HTTP status code of the response.
            - "headers": A dictionary of response headers.
            - "text": The response body as text.
            - "json": The response body parsed as JSON, or None if parsing failed.
    """
    if not isinstance(urls, list) or (
        headers_list
        and (
            not all(isinstance(i, dict) for i in headers_list)
            or not isinstance(headers_list, dict)
        )
    ):
        raise TypeError(
            "urls must be a list and headers must be a list of dictionaries or a dictionary"
        )

    if headers_list is None:
        headers_list = [{}] * len(urls)
    elif isinstance(headers_list, dict):
        headers_list = [headers_list] * len(urls)

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

    tasks = [get_one(url, headers) for url, headers in zip(urls, headers_list)]
    return await asyncio.gather(*tasks)


async def async_post(
    url: str,
    data_list: List[Union[str, Dict]] = None,
    headers_list: Union[List[Dict[str, str]], Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Send asynchronous POST requests to a given URL with multiple sets of data and headers.

    Args:
        url (str): The URL to send the POST requests to.
        data_list (List[Union[str, Dict]], optional): A list of data to be sent in the requests.
            Each element can be either a JSON string or a dictionary. Defaults to None.
        headers_list (Union[List[Dict[str, str]], Dict[str, str]], optional): Headers to include in the requests.
            It can be a list of dictionaries, where each dictionary contains headers for a specific URL,
            or a single dictionary that will be used for all URLs. Defaults to None.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the responses for each URL.
            Each dictionary contains the following keys:
            - "status": The HTTP status code of the response.
            - "headers": A dictionary of response headers.
            - "text": The response body as text.
            - "json": The response body parsed as JSON, or None if parsing failed.

    Raises:
        TypeError: If the data_list is not a list or if the headers_list is not a list of dictionaries or a dictionary.
        TypeError: If the headers_list is a list but not all elements are dictionaries.
        TypeError: If the lengths of data_list and headers_list are not equal.
        ValueError: If any string element in data_list is not a valid JSON string.

    Examples:
        # Example 1: Sending POST requests without data and headers
        responses = await async_post("https://api.example.com/endpoint")

        # Example 2: Sending POST requests with data and headers
        data = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25}
        ]
        headers = {"Authorization": "Bearer token1"}
        responses = await async_post("https://api.example.com/endpoint", data_list=data, headers_list=headers)

        # Example 3: Using async_post with asyncio
        responses = asyncio.run(async_post("https://api.example.com/endpoint"))

        # Example 4: Using async_post with nest_asyncio in notebooks
        import nest_asyncio

        nest_asyncio.apply()
        responses = asyncio.run(async_post("https://api.example.com/endpoint"))
    """
    if not isinstance(data_list, list):
        raise TypeError("data_list must be a list")
    if not isinstance(headers_list, (list, dict)):
        raise TypeError("headers_list must be a list of dictionaries or a dictionary")
    if isinstance(headers_list, list) and (
        not all(isinstance(i, dict) for i in headers_list) or len(data_list) != len(headers_list)
    ):
        raise TypeError(
            "headers_list must be a list of dictionaries and must have the same number of elements as data_list"
        )

    if headers_list is None:
        headers_list = [{}] * len(data_list)
    elif isinstance(headers_list, dict):
        headers_list = [headers_list] * len(data_list)

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
