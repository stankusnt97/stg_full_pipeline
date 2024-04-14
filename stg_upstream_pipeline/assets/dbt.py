
import os
from dagster import AssetExecutionContext, AssetKey, job, GraphIn
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator


from .constants import DBT_DIRECTORY
from ..resources import dbt_resource



class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        # Check: need to add logic to point source of dbt to different thing?
        if resource_type == "source":
            return AssetKey(f"{name}")
        else:
            return super().get_asset_key(dbt_resource_props)



# Refrain from creating extra manifest files
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(["--quiet", "parse"]).wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")



@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

