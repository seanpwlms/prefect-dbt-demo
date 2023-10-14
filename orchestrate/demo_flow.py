from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
from prefect import task, flow
from prefect_snowflake.database import SnowflakeConnector
from datetime import timedelta
from prefect.deployments import run_deployment
import time

# Prefect block for configuration storage and retrieval 📦
snowflake_connector = SnowflakeConnector.load("snowflake-demo-connector")

# Snowflake Task 🏔
@task(name="❄️ Snowflake Task", cache_expiration=timedelta(minutes=30))
def count_recent_cc_records():
    result = snowflake_connector.fetch_one(
        "select count(1) from snowflake_sample_data.tpcds_sf10tcl.call_center where cc_rec_start_date > current_date - 1"
    )
    time.sleep(1)

    return result

# Dynamically build DBT flow 🛠
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
        "name": "📈 DBT Subflow",
        "task_runner": ConcurrentTaskRunner(),
    },
)

@task(name="📊 Update Dashboard")
def update_dashboard(transformed_data):
    print("Updating dashboard with transformed data!")
    time.sleep(3)

# Parent orchestrator flow 🎻
@flow(name="🎻 DBT Orchestrator Flow", log_prints=True, persist_result=True)
def dbt_orchestrator_flow():

    # Snowflake Task 🏔
    fresh_data = count_recent_cc_records()

    if fresh_data is not None:
        # DBT Subflow 🟢
        transformed_data = my_dbt_flow._run()

    else:
        print("No fresh data found, scheduling another run in 15 minutes.") 
        # Schedule another flow run 15 minutes from now. 🔁
        run_deployment('dbt-parent-flow')
    
    # Update Dashboard 📊
    update_dashboard.submit(transformed_data)



if __name__ == "__main__":
    dbt_orchestrator_flow() # Run once for development and testing
    # dbt_orchestrator_flow.serve("my-deployment", interval=1800) # Interval Schedule of 30 minutes