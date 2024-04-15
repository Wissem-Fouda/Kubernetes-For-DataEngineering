from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Define your default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define your DAG
dag = DAG(
    'example_kubernetes_pod',
    default_args=default_args,
    description='A simple DAG to demonstrate KubernetesPodOperator',
    schedule_interval=None
)

# Define the task using KubernetesPodOperator
task1 = KubernetesPodOperator(
    namespace='default',
    image="ubuntu:latest",
    cmds=["echo"],
    arguments=["Hello, Airflow!"],
    name="task-1",
    task_id="task-1",
    get_logs=True,
    dag=dag
)
