from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run
from prefect import flow, task
import time
from prefect_snowflake.database import SnowflakeConnector


snowflake_connector = SnowflakeConnector.load("snowflake-demo-connector")

@flow
def cloud_job(JOB_ID = 424466):
    dbt_cloud_credentials = DbtCloudCredentials.load("dbt-cloud-creds")
    trigger_dbt_cloud_job_run(dbt_cloud_credentials=dbt_cloud_credentials, job_id=JOB_ID)

@task
def count_recent_cc_records():
    result = snowflake_connector.fetch_one(
        "select count(1) from snowflake_sample_data.tpcds_sf10tcl.call_center where cc_rec_start_date > current_date - 1"
    )
    return result

@flow
def daily_job(retries=3):
    while retries > 0:
        fresh_data = count_recent_cc_records()
        if fresh_data is not None:
            cloud_job()
        else:
            time.sleep(5) # sleep for 5 seconds
            retries -= 1
    if retries == 0:
        raise Exception("No fresh data found after three retries")

if __name__ == "__main__":
    daily_job()