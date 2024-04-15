from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator

def print_output(**kwargs):
    print(kwargs['ti'].xcom_pull(task_ids='task-1'))

# Define your default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
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
    namespace='airflow',
    image="python:3.8-slim",
    cmds=["echo"],
    arguments=["Hello, Airflow!"],
    name="task-1",
    task_id="task-1",
    get_logs=True,
    dag=dag
)

# Task to print the output
print_output_task = PythonOperator(
    task_id='print_output_task',
    python_callable=print_output,
    provide_context=True,
    dag=dag
)

task1 >> print_output_task
