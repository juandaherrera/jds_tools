# JD Science Tools <!-- omit from toc -->

[![Python](https://img.shields.io/badge/Python-3.10+-5646ED?style=for-the-badge&logo=python&logoColor=white&labelColor=101010)](https://python.org)
[![Tag](https://img.shields.io/github/v/tag/juandaherrera/jds_tools?style=for-the-badge&labelColor=101010)](https://github.com/juandaherrera/jds_tools/releases)

[![Jinja2](https://img.shields.io/badge/Jinja2-3.0+-B41717?style=for-the-badge&logo=jinja&logoColor=white&labelColor=101010)](https://pypi.org/project/Jinja2/)
[![gspread](https://img.shields.io/badge/gspread-6.0.0+-1DBF73?style=for-the-badge&logo=googlesheets&logoColor=white&labelColor=101010)](https://pypi.org/project/gspread/)
[![aiohttp](https://img.shields.io/badge/aiohttp-3.9.5-007396?style=for-the-badge&logo=aiohttp&logoColor=white&labelColor=101010)](https://pypi.org/project/aiohttp/)
[![Snowflake](https://img.shields.io/badge/snowflake_sqlalchemy-1.5.3-1E90FF?style=for-the-badge&logo=snowflake&logoColor=white&labelColor=101010)](https://pypi.org/project/snowflake-connector-python/)

## Description <!-- omit from toc -->
JD Science Tools is a utility library designed for Data Science, Data Engineering, and Python Development projects. It provides useful tools and functions to facilitate work in these fields. This project is intended for personal use but is available for anyone interested in exploring or contributing.

## Table of Contents <!-- omit from toc -->
- [Installation](#installation)
- [Usage](#usage)
  - [JinjaHook](#jinjahook)
  - [SnowflakeHook](#snowflakehook)
  - [GoogleSheetsHook](#googlesheetshook)
  - [Asynct Requests](#asynct-requests)
  - [Path Utils](#path-utils)
- [License](#license)
- [Contributing](#contributing)
- [Contact](#contact)


## Installation
To install JD Science Tools from PyPI, run the following command in your terminal:

```bash
pip install jds_tools
```

Ensure that you have pip installed on your system and that you are using Python 3.10 or higher.

## Usage

### JinjaHook
Imagine you have the following project structure:
```
.
├── project_folder
│ ├── queries
│ │ ├── my_query.sql
│ ├── main.py
```

**Example SQL Template (`my_query.sql`)**

Here is an example of a SQL template using Jinja:

```jinja 
SELECT 
    {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ schema }}.{{ table_name }}
```

**Rendering the Query in `main.py`**

To render your query with Jinja, you can use the JinjaHook from the jds_tools package as follows:

```python
import os
from jds_tools.hooks import JinjaHook

# Initialize the JinjaHook with the directory containing your SQL templates
jinja = JinjaHook(os.path.join(os.getcwd(), "queries"))

# Define the parameters to pass into the template
params = dict(
    columns=["column1", "column2", "column3"],
    schema="my_schema",
    table_name="my_table"
)

# Render the SQL query with the parameters
query = jinja.render("my_query.sql", params)
print(query)
```
**Resulting SQL Query**

After rendering, you obtain the following SQL query:

```sql
SELECT
    colum1,
    column2,
    column3
FROM my_schema.my_table
```

### SnowflakeHook

The following example demonstrates how to use the `SnowflakeHook` from the `jds_tools` package to interact with a Snowflake database. You will learn how to fetch data, upload data, and run multiple statement queries.

```python
import os
from jds_tools.hooks import SnowflakeHook

# Initialize the SnowflakeHook with environment variables
snowflake_hook = SnowflakeHook(
    os.getenv('SNOWFLAKE_ACCOUNT'),
    os.getenv('SNOWFLAKE_USER'),
    os.getenv('SNOWFLAKE_PASSWORD'),
    os.getenv('SNOWFLAKE_WAREHOUSE'),
    os.getenv('SNOWFLAKE_DATABASE'),
)

# Fetch data from Snowflake
result = snowflake_hook.fetch_data("SELECT * FROM your_table")

# Uploading data
snowflake_hook.role = "your_role_with_write_permissions"
snowflake_hook.upload_data(result, "your_table", "your_schema", "replace")

# Running a multiple statement query
query = """
BEGIN;
USE DATABASE your_database;
USE SCHEMA your_schema;

CREATE OR REPLACE TABLE your_table AS
SELECT 1 AS TEST_COLUMN;
COMMIT;
"""
snowflake_hook.execute_statement(query)
```

### GoogleSheetsHook
The `GoogleSheetsHook` class provides an interface for interacting with Google Sheets. Here are some examples of how to use it:
```python
from jds_tools.hooks import GoogleSheetsHook

credentials_path = os.path.join(project_root, "your_google_key.json")
google_hook = GoogleSheetsHook("your_google_spreadsheet_id", credentials=credentials_path)
```

You can also insert the credentials as a string with a valid json or in a dictionary, like this:
```python
credentials = {
    'type': 'service_account',
    'project_id': 'my_project',
    'private_key_id': 'my_private_key_id',
    'private_key': 'my_private_key',
    'client_email': 'my_client_email',
    'client_id': 'my_client_id',
    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
    'token_uri': 'https://oauth2.googleapis.com/token',
    'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
    'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/my_client_email'
}
google_hook = GoogleSheetsHook('your_google_spreadsheet_id', credentials=credentials, credentials_type="variable")
```

And then use the Hook like this:
```python
# Reading Data
g_data = google_hook.read("your_worksheet_name", return_df=True)

# Writing Data
google_hook.write("your_worksheet_name", g_data)

# Appending Data
google_hook.append("your_worksheet_name", g_data)

# Cleaning Data
google_hook.clear("your_worksheet_name", "A2:B99")
```

### Asynct Requests
#### async_get <!-- omit from toc -->

The `async_get` function sends asynchronous GET requests to multiple URLs and returns the responses.

```python
import asyncio
from jds_tools.utils.async_requests import async_get

# Define the URLs to send GET requests to
urls = ["https://api.example.com/endpoint1", "https://api.example.com/endpoint2"]

# Define the headers to include in the requests
headers = {"Authorization": "Bearer token"}

# Send the requests and get the responses
responses = asyncio.run(async_get(urls, headers))

# Print the responses (dict with status, headers, text, json)
for response in responses:
    print(response)
```

#### async_post <!-- omit from toc -->
The `async_post` function sends asynchronous POST requests to a given URL with multiple sets of data and headers.

```python
import asyncio
from jds_tools.utils.async_requests import async_post

# Define the URL to send POST requests to
url = "https://api.example.com/endpoint"

# Define the data to be sent in the requests
data = [
    {"name": "John", "age": 30},
    {"name": "Jane", "age": 25}
]

# Define the headers to include in the requests
headers = {"Content-Type": "application/json"}

# Send the requests and get the responses
responses = asyncio.run(async_post(url, data, headers))

# Print the responses (dict with status, headers, text, json)
for response in responses:
    print(response["status"])
    print(response["json"])
```

### Path Utils
#### add_project_root_to_sys_path <!-- omit from toc -->
This function adds the project root directory to sys.path if it's not already there. If project_root is not provided, it will start from the current directory and search upwards for a directory containing any of the files in [ROOT_FILES](jds_tools/utils/path_utils.py). If it doesn't find a directory containing any of those files within max_depth directories, it raises an exception. If recursive_search is False, it won't search upwards and will just use the current directory as the project root.

```python
from jds_tools.utils.path_utils import add_project_root_to_sys_path

# Add the current directory's parent directory to sys.path
add_project_root_to_sys_path()

# If you know the project root and don't want to search recursively
add_project_root_to_sys_path(project_root='/path/to/your/project', recursive_search=False)

# If you want to limit the depth of the recursive search
add_project_root_to_sys_path(max_depth=3)
```

## License
This project is licensed under the [MIT License](LICENSE).

## Contributing
Contributions are welcome! If you would like to contribute to this project, please fork the repository and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## Contact
If you have any questions, suggestions, or just want to connect, feel free to reach out to me via:
| Platform | Contact |
| --- | -------- |
| Email | juandaherreparra@gmail.com | 
| LinkedIn | [![Linkedin: Juan David Herrera](https://img.shields.io/badge/-Juan_David_Herrera-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/juan-david-herrera/)](https://www.linkedin.com/in/juan-david-herrera/) |