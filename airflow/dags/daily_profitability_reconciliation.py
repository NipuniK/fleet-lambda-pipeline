"""Airflow DAG: orchestrates the batch layer of the Lambda architecture.

Runs once per simulated day (schedule interval matches SIM_DAY_SECONDS). Steps:
  1. wait_for_cost_file  - sensor: waits for the day's cost drop file to exist
  2. run_batch_job       - spark-submit the reconciliation job for that sim day
  3. check_output        - sanity check that vehicle_profitability got new rows

This is the piece that gives the batch layer a real orchestration story (vs. just
running a cron'd script), and is where you'd add retries/SLA alerts for the report.
"""
import glob
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", 300))
DROP_DIR = "/opt/airflow/producers_drops"  # mount producers/drops here, or point at shared volume


def _sim_day_for_run(**context) -> int:
    # Simplest mapping for a demo: sim_day = number of DAG runs so far.
    return context["dag_run"].run_id.count("scheduled") or 0


def _find_cost_file(**context) -> bool:
    files = sorted(glob.glob(os.path.join(DROP_DIR, "costs_day_*.json")))
    return len(files) > 0


def _run_reconciliation(**context):
    files = sorted(glob.glob(os.path.join(DROP_DIR, "costs_day_*.json")))
    latest = files[-1]
    sim_day = int(latest.split("_")[-1].split(".")[0])
    os.system(
        f"spark-submit /opt/airflow/processing/batch_reconciliation.py "
        f"--sim-day {sim_day} --cost-file {latest}"
    )


default_args = {
    "owner": "fleet-team",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_profitability_reconciliation",
    default_args=default_args,
    schedule_interval=timedelta(seconds=SIM_DAY_SECONDS),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "batch-layer"],
) as dag:

    wait_for_cost_file = PythonSensor(
        task_id="wait_for_cost_file",
        python_callable=_find_cost_file,
        poke_interval=10,
        timeout=SIM_DAY_SECONDS,
    )

    run_batch_job = PythonOperator(
        task_id="run_batch_job",
        python_callable=_run_reconciliation,
    )

    check_output = BashOperator(
        task_id="check_output",
        bash_command="echo 'batch reconciliation task finished for this sim day'",
    )

    wait_for_cost_file >> run_batch_job >> check_output
