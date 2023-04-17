#!/usr/bin/env python


from setuptools import setup, find_packages
import shutil
import os

if os.path.isdir('build'):
    shutil.rmtree('build')

setup(
    name='appsflyer',
    version='1.0',
    description='Singer.io tap for extracting data from the AppsFlyer API',
    author='Stitch, Inc.',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['appsflyer'],
    packages=['appsflyer'],
    package_data={
        'appsflyer/schemas/raw_data': [
            'installations.json',
            'in_app_events.json'
        ],
    },
    include_package_data=True,
)


# clean-up
if os.path.isdir('build'):
    shutil.rmtree('build')
# shutil.rmtree('appsflyer.egg-info')