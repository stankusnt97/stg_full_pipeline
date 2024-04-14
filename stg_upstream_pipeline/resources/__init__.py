from dagster_databricks import databricks_client

from dagster_dbt import DbtCliResource

from ..assets.constants import DBT_DIRECTORY

# Databricks connection
databricks_client_instance = databricks_client.configured(
    {
        "host": {"env": "DATABRICKS_HOST"},
        "token": {"env": "DATABRICKS_TOKEN"},
    }
)

# Bringing in DBT resources
dbt_resource = DbtCliResource(
    project_dir=DBT_DIRECTORY,
)