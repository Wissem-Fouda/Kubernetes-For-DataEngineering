from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
# Define default arguments for the DAG => i'm editing this DAG to check GitSync updates
default_args = {
    'owner': 'Wissem Fouda',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
# Define the DAG
dag = DAG(
    'example_kubernetes_pod',
    default_args=default_args,
    description='A simple DAG to demonstrate KubernetesPodOperator',
    schedule_interval=None
)
# Define task1 using KubernetesPodOperator
task1 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/tasks:1.0",
    name="task-1",
    task_id="task-1",
    get_logs=True,
    dag=dag
)
# Define task2 using KubernetesPodOperator
task2 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/demorepo:1.0",
    name="task-2",
    task_id="task-2",
    get_logs=True,
    dag=dag
)

#bash_task = BashOperator(
#    task_id="bash_task",
#    bash_command="echo $MY_VAR",
#    env={"MY_VAR": "Hello World"}
#)

# Set task dependencies
task1 >> task2 
