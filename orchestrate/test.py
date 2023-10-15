from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
from prefect import task, flow


@task
def upstream_task():
    print("upstream task")


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


@task
def downstream_task():
    print("downstream task")


@flow
def another_flow():
    up = upstream_task.submit()
    f = my_dbt_flow(wait_for=[up])
    downstream_task.submit(wait_for=[f])


# @flow(log_prints=True)
# def hi_parent_flow():

#     upstream_task()

#     a = my_dbt_flow()


if __name__ == "__main__":
    # hi_parent_flow()
    another_flow()
