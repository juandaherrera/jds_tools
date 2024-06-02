import os

from setuptools import find_packages, setup

from jds_tools import __VERSION__


def readme() -> str:
    """Utility function to read the README.md.

    Used for the `long_description`. It's nice, because now
    1) we have a top level README file and
    2) it's easier to type in the README file than to put a raw string in below.

    Args:
        nothing

    Returns:
        String of readed README.md file.
    """
    return open(os.path.join(os.path.dirname(__file__), 'README.md')).read()


setup(
    name="jds_tools",
    version=__VERSION__,
    author="Juan David Herrera",
    author_email="juandaherreparra@gmail.com",
    description="Librería de utilidades diseñada para proyectos de Data Science, Data Engineering y Python Development",
    python_requires=">=3.10",
    license="MIT",
    packages=find_packages(include=["jds_tools.*"]),
    install_requires=[
        "snowflake-sqlalchemy==1.5.3",
        "pandas==2.0.0",
        "jinja2==3.1.3",
        "aiohttp==3.9.5",
    ],
    long_description=readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/juandaherrera/jds_tools",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
