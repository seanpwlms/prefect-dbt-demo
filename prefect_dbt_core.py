from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow

@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt build -t prod"],
        project_dir="prefect_demo",
        profiles_dir="~/.dbt"
    ).run()

if __name__ == "__main__":
    trigger_dbt_flow()
