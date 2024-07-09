from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


default_args = {
    'owner': 'Wissem Fouda',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
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



run_sql_insert_container = KubernetesPodOperator(
    namespace='airflow',
    image='foudazdocker/mysql:2.0', 
    name="run-sql-insert-container",
    task_id="run_sql_insert_container_task",
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)


run_sql_insert_container
