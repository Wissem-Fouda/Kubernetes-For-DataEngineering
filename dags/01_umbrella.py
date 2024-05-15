from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator




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
    description='A simple DAG to demonstrate KubernetesPodOperator & to test sync',
    schedule_interval=None
)

# Define the task using KubernetesPodOperator
task1 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/tasks:1.0",
    name="task-1",
    task_id="task-1",
    get_logs=True,
    dag=dag
)


task2 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/demorepo:1.0",
    name="task-2",
    task_id="task-2",
    get_logs=True,
    #volumes=[{"name": "persistent-storage", "persistentVolumeClaim": {"claimName": "pvc-tasks"}}],
    #volume_mounts=[{"mountPath": "/mnt/persistent_volume", "name": "persistent-storage"}],
    dag=dag
)

# Set task dependencies
task1 >> task2
