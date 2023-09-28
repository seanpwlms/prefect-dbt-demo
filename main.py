from prefect import flow, task
from prefect_dbt.cli import DbtCliProfile
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect_dbt.cli.configs import SnowflakeTargetConfigs
from dbt import Dbt

snowflake_target_configs = SnowflakeTargetConfigs.load("dbt-snowflake-config")

dbt_cli_profile = DbtCliProfile.load("dbt-demo-profile").get_profile()


@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt run"],
        project_dir="prefect_demo",
        dbt_cli_profile=dbt_cli_profile,
        overwrite_profiles=True
    ).run()
    return result



trigger_dbt_flow()


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

# @flow
# def dbt_jaffle_shop():
#     dbt = Dbt.load("dbt-custom-block")
#     dbt.dbt_cli("dbt compile")
#     dbt.dbt_run_from_manifest()


# if __name__ == "__main__":
#     dbt_jaffle_shop()

