# setup.py
'''
Setup tools
'''
import re
import subprocess
from setuptools import setup, find_packages

NAME = 'falcano'
VERSION = '0.1'
AUTHOR = 'Eric Walker'
AUTHOR_EMAIL = 'ewalker3@mmm.com'
DESCRIPTION = 'Falcono'
URL = 'https://github.com/3mcloud/falcano'
REQUIRES = [
    'bottle==0.12.18',
    'cache',
    'stringcase',
]
REQUIRES_TEST = [
    'rsa>=4.3',
    'PyYAML>=5.3.1',
    'pylint>=2.5.0',
    'pytest>=5.4.1',
    'pytest-cov>=2.8.1',
    'bandit>=1.6.2',
    'safety>=1.8.7',
    'boto3',
    'paste',
    'ptvsd',
]

REQUIRES_DOCS = [

]
LONG_DESCRIPTION = 'Single-table design Dynamo library'


setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url=URL,
    packages=find_packages(exclude=("tests", "tests.*")),
    install_requires=REQUIRES,
    extras_require={
        'dev': REQUIRES_TEST,
    },
    include_package_data=True
)
