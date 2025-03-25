from setuptools import setup, find_packages

setup(
    name="tap-appsflyer",
    version="0.3.0",
    description="Singer.io tap for extracting data from appsflyer API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_appsflyer"],
    install_requires=[
        "attrs==25.1.0",
        "singer-python==6.1.0",
        "requests==2.32.3",
        "backoff==2.2.1",
    ],
    entry_points="""
        [console_scripts]
        tap-appsflyer=tap_appsflyer:main
    """,
    packages=find_packages(),
    package_data={
        "tap_appsflyer": ["schemas/*.json"],
    },
    include_package_data=True,
)
