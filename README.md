# Getting Started with dbt, Snowflake, and Prefect

This repository contains code samples to demonstrate three ways to use Prefect and dbt. It relies on generic sample Snowflake tables, so the whole thing can be run for free during a Snowflake and dbt Cloud trial period.  See the [tutorial](tutorial.md) for more detail and a full explanation.

## 1. dbt Cloud with prefect-dbt

`prefect_dbt_cloud.py` has a simple example of running a dbt Cloud job with Prefect.

## 2. dbt Core with prefect-dbt

`prefect_dbt_core.py` has a simple example of running dbt Core.

## 3. dbt Core with prefect-dbt-flow

`prefect_dataroots_flow.py` contains an example using a library created by Dataroots that will separate individual dbt models and tests as Prefect tasks for more granularity and better visualization within Prefect.


## Trivial example of waiting for late-arriving data

`daily_dbt_job.py` has a simple example of how we might check for fresh data before kicking off a dbt Cloud job.