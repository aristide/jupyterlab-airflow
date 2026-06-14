"""A tiny example DAG so the jupyterlab-airflow extension has something local to
list, pause, and trigger during development. Drop more .py files into this
folder (mounted at /opt/airflow/dags) and they will appear after the
dag-processor picks them up.
"""

from __future__ import annotations

import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="jupyterlab_airflow_demo",
    description="Demo DAG shipped with the jupyterlab-airflow devcontainer",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["jupyterlab-airflow", "demo"],
):

    @task
    def say_hello() -> str:
        print("Hello from the jupyterlab-airflow demo DAG!")
        return "hello"

    @task
    def shout(greeting: str) -> None:
        print(greeting.upper())

    shout(say_hello())
