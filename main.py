from prefect import flow, task
from prefect_dbt.cli import DbtCliProfile
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from prefect.artifacts import create_markdown_artifact
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run



from dbt import Dbt
import time
import json
import pandas as pd

@flow
def cloud_job():
    dbt_cloud_credentials = DbtCloudCredentials.load("dbt-cloud-creds")
    trigger_dbt_cloud_job_run(dbt_cloud_credentials=dbt_cloud_credentials, job_id=424466)

snowflake_target_configs = SnowflakeTargetConfigs.load("dbt-snowflake-config")

dbt_cli_profile = DbtCliProfile.load("dbt-demo-profile").get_profile()


@task
def airbyte_sync():
    time.sleep(8)


# @flow
# def trigger_dbt_flow() -> str:
#     result = DbtCoreOperation(
#         commands=["pwd", "dbt debug", "dbt run"],
#         project_dir="prefect_demo",
#         dbt_cli_profile=dbt_cli_profile,
#         overwrite_profiles=True
#     ).run()
#     return result



# trigger_dbt_flow()


# dbt_cli_profile = DbtCliProfile(
#     name="prefect_demo",
#     target="dev",
#     target_configs=snowflake_target_configs,
# )

# dbt_init = DbtCoreOperation(
#     commands=["dbt debug", "dbt list"],
#     dbt_cli_profile=dbt_cli_profile
# )
# dbt_init.run()

# @flow
# def trigger_dbt_flow() -> str:
#     result = DbtCoreOperation(
#         commands=["pwd", "dbt debug", "dbt run"],
#         project_dir="prefect_demo",
#         profiles_dir="~/.dbt"
#     ).run()
# #     return result
# if __name__ == "__main__":
#     trigger_dbt_flow.serve(name="dbt-demo-flow")

@flow
def dbt_jaffle_shop():
    dbt = Dbt.load("dbt-custom-block")
    dbt.dbt_cli("dbt compile")
    dbt.dbt_run_from_manifest()



@task
def parse_results():
    with open(f"prefect_demo/target/run_results.json") as f:
        data = json.load(f)
    df = pd.DataFrame.from_dict(data['results'])
    df_md = df.to_markdown(index=False)
    create_markdown_artifact(
        key="gtm-report",
        markdown=df_md,
        description="run_results",
    )

@flow
def elt():
    airbyte_sync()
    #dbt_jaffle_shop()
    cloud_job()
    parse_results()

@flow
def get_result():
    parse_results()

if __name__ == "__main__":
    elt()

