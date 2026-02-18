"""
Airflow DAG: Financial Data Platform

Orchestrates the daily ELT pipeline. This DAG is designed to run on:
- Self-hosted Airflow on Cloud Run (free tier demo)
- GCP Composer (production deployment)

Pipeline phases:
1. Trigger Fivetran syncs for QuickBooks, Stripe, Salesforce (parallel)
2. Run custom Python extractors for NetSuite, Plaid via Cloud Run Jobs (parallel)
3. Wait for all ingestion to complete
4. Run dbt seed -> build -> snapshot -> test
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)
from airflow.utils.task_group import TaskGroup

# Fivetran operator requires: pip install airflow-provider-fivetran
try:
    from fivetran_provider.operators.fivetran import FivetranOperator
    from fivetran_provider.sensors.fivetran import FivetranSensor
    FIVETRAN_AVAILABLE = True
except ImportError:
    FIVETRAN_AVAILABLE = False

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt"
DBT_PROFILES_DIR = "/opt/airflow/.dbt"
GCP_PROJECT = "{{ var.value.gcp_project_id }}"
GCP_REGION = "{{ var.value.gcp_region }}"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id="financial_data_platform",
    default_args=default_args,
    description="Daily ELT: Fivetran + custom extractors -> dbt build -> tests",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["financial", "elt", "dbt", "fivetran"],
    max_active_runs=1,
) as dag:

    # ---------------------------------------------------------------
    # PHASE 1a: Fivetran-managed sources (QuickBooks, Stripe, SF)
    # ---------------------------------------------------------------
    with TaskGroup("fivetran_sync") as fivetran_group:
        if FIVETRAN_AVAILABLE:
            for connector_name, connector_id_var in [
                ("quickbooks", "fivetran_qb_connector_id"),
                ("stripe", "fivetran_stripe_connector_id"),
                ("salesforce", "fivetran_sf_connector_id"),
            ]:
                trigger = FivetranOperator(
                    task_id=f"trigger_{connector_name}",
                    fivetran_conn_id="fivetran_default",
                    connector_id=f"{{{{ var.value.{connector_id_var} }}}}",
                )
                wait = FivetranSensor(
                    task_id=f"wait_{connector_name}",
                    fivetran_conn_id="fivetran_default",
                    connector_id=f"{{{{ var.value.{connector_id_var} }}}}",
                    poke_interval=60,
                    timeout=1800,
                )
                trigger >> wait

    # ---------------------------------------------------------------
    # PHASE 1b: Custom extractors (NetSuite, Plaid) via Cloud Run Jobs
    # ---------------------------------------------------------------
    with TaskGroup("custom_extract") as extract_group:
        extract_netsuite = CloudRunExecuteJobOperator(
            task_id="netsuite",
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job_name="extract-netsuite",
        )

        extract_plaid = CloudRunExecuteJobOperator(
            task_id="plaid",
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job_name="extract-plaid",
        )

    # ---------------------------------------------------------------
    # PHASE 2: dbt transformation
    # ---------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILES_DIR} --target prod",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir {DBT_PROFILES_DIR} --target prod",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROFILES_DIR} --target prod",
    )

    # Elementary report generation
    elementary_report = BashOperator(
        task_id="elementary_report",
        bash_command=f"cd {DBT_PROJECT_DIR} && edr report --profiles-dir {DBT_PROFILES_DIR}",
        trigger_rule="all_done",  # Run even if tests fail
    )

    # ---------------------------------------------------------------
    # DAG DEPENDENCIES
    # ---------------------------------------------------------------
    [fivetran_group, extract_group] >> dbt_seed >> dbt_build >> dbt_snapshot >> elementary_report
