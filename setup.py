#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-trello",
    version="1.1.1",
    description="Singer.io tap for extracting data from Trello API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_trello"],
    install_requires=[
        "singer-python==6.3.0",
        "requests==2.32.5",
        "requests-oauthlib==1.3.0",
        "backoff==2.2.1"
    ],
    extras_require={
        'dev': [
            'ipdb',
            'pylint',
            'pytest'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-trello=tap_trello:main
    """,
    packages=["tap_trello"],
    package_data = {
        "tap_trello": ["tap_trello/schemas/*.json"]
    },
    include_package_data=True,
)
