# airflow-studio: managed  studio=0.1.0  sha256:f865dfc1f401ea71  dag_id=HELLO  afdag_id=4eec2894-642f-45bd-a969-6343a1cf4020  syntax=taskflow  code=sha256:fa829500b5f3c4457d43ae7a1d056c273875b9aa5a6728c9b9377c989ddf53a2
from datetime import datetime, timedelta
from airflow.sdk import dag, task

@dag(
    dag_id='HELLO',
    schedule='@once',
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(seconds=300)},
)
def HELLO():
    @task.bash(task_id='bash_1')
    def bash_1():
        return 'echo "Hello"'

    bash_1_task = bash_1()

HELLO()
