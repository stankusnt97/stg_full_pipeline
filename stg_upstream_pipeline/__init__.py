from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job
)

from .assets import dbt, stg_nodes, physical_therapy_evals
from .resources import databricks_client_instance, dbt_resource
#from .jobs import stg_nodes, physical_therapy_evals


dbt_analytics_assets = load_assets_from_modules(modules=[dbt]) # Load the assets from the file



defs = Definitions(
    assets=[stg_nodes,
            physical_therapy_evals,
            *dbt_analytics_assets],
    resources={
        "dbt": dbt_resource,
    },
)


