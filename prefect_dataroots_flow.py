from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject

my_dbt_flow = dbt_flow(
    project=DbtProject(
        name="sample_project",
        project_dir=Path() / "prefect_demo",
        profiles_dir=Path.home() / ".dbt",
    ),
    profile=DbtProfile(
        target="prod",
    ),
    flow_kwargs={
        "task_runner": SequentialTaskRunner(),
    },
)

if __name__ == "__main__":
    my_dbt_flow()