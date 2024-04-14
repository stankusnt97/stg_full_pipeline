from setuptools import find_packages, setup

setup(
    name="stg_upstream_pipeline",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-databricks",
        "dagster-databricks",
        "dagster_databricks",
        "dagster-pipes",
        "databricks-sdk",
        "pandas",
        "requests",
    ],
    extras_require={"dev": ["dagster-webserver"]},
)
