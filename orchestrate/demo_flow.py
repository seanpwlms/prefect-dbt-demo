from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
from prefect import task, flow

@task
def upstream_task():
    print('upstream task')

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

@flow
def my_subflow():
    print('This is a subflow')


@flow(log_prints=True)
def parent_flow():

    upstream_task()

    my_dbt_flow._run()

    my_subflow()



if __name__ == "__main__":
    parent_flow()