from setuptools import setup, find_packages
from typing import List





# Declaring variables for the setup functions

PROJECT_NAME = 'RealTime IOT ETL Pipeline'
VERSION = '0.0.1'
AUTHOR = 'Naman Jain'
DESCRIPTION = 'Data is ingested From cassandra to s3 bucket for Analysis'


setup(
    name=PROJECT_NAME,
    version=VERSION,
    author=AUTHOR,
    description=DESCRIPTION,
    packages=find_packages
)