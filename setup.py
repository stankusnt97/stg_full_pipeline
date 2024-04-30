from setuptools import find_packages, setup

setup(
    name="stg_upstream_pipeline",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-databricks",
        "dagster-embedded-elt",
        "dagster-databricks",
        "dagster_databricks",
        "dagster-pipes",
        "databricks-sdk",
        "pandas",
        "requests",
        "google-api-python-client"
    ],
    extras_require={"dev": ["dagster-webserver"]},
)
