from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
from prefect import task, flow
from prefect_snowflake.database import SnowflakeConnector
from datetime import timedelta
from prefect.deployments import run_deployment
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
import time
import datetime

# Prefect block for configuration storage and retrieval 📦
snowflake_connector = SnowflakeConnector.load("snowflake-demo-connector")


# Airbyte Connection Task 🐙
@task(name="🐙 Airbyte Connection Task")
def airbyte_connection_task():
    print("Running Airbyte Task 🐙")
    time.sleep(3)
    return {"airbyte_data": "airbyte_data"}


# Snowflake Task 🏔
@task(name="❄️ Snowflake Data Freshness Check", cache_expiration=timedelta(minutes=30))
def count_recent_cc_records():
    print("Running Snowflake Task ❄️")
    result = snowflake_connector.fetch_one(
        "select count(1) from snowflake_sample_data.tpcds_sf10tcl.call_center where cc_rec_start_date > current_date - 1"
    )
    time.sleep(1)

    return result


# Dynamically build dbt subflow 🛠
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
        "name": "📈 dbt subflow",
        "task_runner": ConcurrentTaskRunner(),
    },
)


@task(name="📊 Update Dashboard")
def update_dashboard(transformed_data):
    print("Updating dashboard with transformed data! 📊")
    time.sleep(3)


@task(name="💌 Send Confirmation Emails")
def confirmation_emails():
    print("Sending confirmation emails! 💌")
    time.sleep(3)


@flow(name="📦 Shipping Flow")
def shipping_flow():
    print("Kicking off the Shipping Flow 📦")
    time.sleep(3)
    return {"shipping_data": "shipping_data"}


# ----------------------------------------------


# parent orchestrator flow 🎻
@flow(name="🎻 dbt Orchestrator Flow", log_prints=True, persist_result=True)
def dbt_orchestrator_flow():
    # airbyte connection task 🐙
    data_transfer = airbyte_connection_task.submit()

    # snowflake task 🏔
    fresh_data = count_recent_cc_records.submit(wait_for=[data_transfer])

    if fresh_data is not None:
        # dbt subflow 🟢
        transformed_data = my_dbt_flow(wait_for=[fresh_data])

    else:
        print("No fresh data found, scheduling another run in 15 minutes.")
        # Schedule another flow run 15 minutes from now. 🔁
        fifteen_minutes_from_now = datetime.now() + timedelta(minutes=15)
        run_deployment("dbt-parent-flow", scheduled_time=fifteen_minutes_from_now)

    # update dashboard 📊
    update_dashboard.submit(transformed_data)

    # kick off shipping subflow 📦
    shipping_data = shipping_flow(wait_for=[transformed_data])

    # send confirmation emails 💌
    confirmation_emails.submit(wait_for=shipping_data)


if __name__ == "__main__":
    # dbt_orchestrator_flow()  # Run once for development and testing
    dbt_orchestrator_flow.serve("my-deployment", interval=1800, tags=['serve']) # Interval Schedule of 30 minutes
