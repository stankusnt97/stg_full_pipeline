from ..jobs import stg_nodes_op, physical_therapy_evals_op

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

@graph_asset(
          resource_defs={"databricks": databricks_client_instance}
          )
def stg_nodes():
    return stg_nodes_op()

@graph_asset(
          resource_defs={"databricks": databricks_client_instance}
          )
def physical_therapy_evals():
    return physical_therapy_evals_op()