from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run
from prefect import flow



@flow
def cloud_job(JOB_ID = 424466):
    dbt_cloud_credentials = DbtCloudCredentials.load("dbt-cloud-creds")
    trigger_dbt_cloud_job_run(dbt_cloud_credentials=dbt_cloud_credentials, job_id=JOB_ID)

if __name__ == "__main__":
    cloud_job()