from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run
from prefect import flow, task
from prefect_snowflake.database import SnowflakeConnector
from prefect.states import Failed

snowflake_connector = SnowflakeConnector.load("snowflake-demo-connector")

@flow
def cloud_job(JOB_ID = 424466):
    dbt_cloud_credentials = DbtCloudCredentials.load("dbt-cloud-creds")
    trigger_dbt_cloud_job_run(dbt_cloud_credentials=dbt_cloud_credentials, job_id=JOB_ID)

@task
def count_recent_cc_records():
    result = snowflake_connector.fetch_one(
        "select cc_rec_start_date > current_date - 1 as is_fresh, max(cc_rec_start_date) as max_date from snowflake_sample_data.tpcds_sf10tcl.call_center group by 1"
    )
    return result

@flow(retries = 3, retry_delay_seconds = 3, log_prints = True)
def daily_job():
    fresh_data = count_recent_cc_records()
    if fresh_data[0]:
        cloud_job()
    else:
        return Failed(message = f'Stale data: most recent date is {str(fresh_data[1])}')

if __name__ == "__main__":
    daily_job()
