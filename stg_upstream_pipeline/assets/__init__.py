from ..jobs import stg_nodes_op, physical_therapy_evals_op

import os
from dagster import op, job, graph, GraphOut, graph_asset, graph_multi_asset, AssetExecutionContext, AssetOut
import pandas as pd
from typing import Union

# DLT packages
from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt_sources.google_sheets import *

# Extra dependencies needed for retrieving environment variables
from dotenv import load_dotenv

load_dotenv()

# Instatiate
GOOGLE_SHEETS_PROJECT_ID = os.getenv('GOOGLE_SHEETS_PROJECT_ID')
GOOGLE_SHEETS_CLIENT_EMAIL = os.getenv('GOOGLE_SHEETS_CLIENT_EMAIL')
GOOGLE_SHEETS_PRIVATE_KEY = os.getenv('GOOGLE_SHEETS_PRIVATE_KEY')
GOOGLE_SHEETS_SPREADSHEET_URL_OR_ID = os.getenv('GOOGLE_SHEETS_SPREADSHEET_URL_OR_ID')
GOOGLE_SHEETS_RANGE_NAMES = os.getenv('GOOGLE_SHEETS_SPREADSHEET_URL_OR_ID')
GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS')

# Dagster packages
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


@dlt_assets(
    dlt_source=google_spreadsheet(
        spreadsheet_url_or_id=GOOGLE_SHEETS_SPREADSHEET_URL_OR_ID, 
        credentials=GOOGLE_SHEETS_CREDENTIALS,  # Assuming the credentials instantiation is needed here
        max_api_retries=5
    ),
    dlt_pipeline=pipeline(
        pipeline_name="google_sheets_pipeline",
        dataset_name="devices",
        destination="databricks"
    ),
    name="devices_google_sheets",
    group_name="google_sheets"
)

def dagster_google_sheets_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

# @dlt.source
# def google_spreadsheet(
#     spreadsheet_url_or_id: str = dlt.config.value,
#     range_names: Sequence[str] = dlt.config.value,
#     credentials: Union[
#         GcpOAuthCredentials, GcpServiceAccountCredentials
#       ] = dlt.secrets.value,
#     get_sheets: bool = False,
#     get_named_ranges: bool = True,
#     max_api_retries: int = 5,
# ) -> Iterable[DltResource]:
