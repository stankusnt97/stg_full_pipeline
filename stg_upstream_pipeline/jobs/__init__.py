import os
from dagster import op, job, graph, GraphOut, graph_asset, graph_multi_asset, AssetExecutionContext, AssetOut
import pandas as pd

from dagster_databricks import databricks_client, create_databricks_run_now_op
from databricks_cli.sdk import DbfsService


databricks_client_instance = databricks_client.configured(
    {
        "host": {"env": "DATABRICKS_HOST"},
        "token": {"env": "DATABRICKS_TOKEN"},
    }
)


    

# Op factory
stg_nodes_op = create_databricks_run_now_op(
        databricks_job_id=712046683431491,
        name="stg_nodes_op" # Custom op name
    )

physical_therapy_evals_op = create_databricks_run_now_op(
        databricks_job_id=161287971241025,
        name="physical_therapy_evals_op" # Custom op name
    )


