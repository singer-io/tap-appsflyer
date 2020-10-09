#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-appsflyer',
    version='0.0.12',
    description='Singer.io tap for extracting data from the AppsFlyer API',
    author='Stitch, Inc.',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_appsflyer'],
    install_requires=[
        'attrs',
        'singer-python',
        'requests',
        'backoff',
    ],
    extras_require={
        'dev': [
            'bottle',
            'faker'
        ]
    },
    entry_points={
        'console_scripts': [
            'tap-appsflyer=tap_appsflyer:main'
        ]
    },
    packages=['tap_appsflyer'],
    package_data={
        'tap_appsflyer/schemas/': [
            'installations.json',
            'in_app_events.json',
            'organic_installs.json',
            'daily_report.json'
        ],
    },
    include_package_data=True,
)
