from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'Fouda',
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG('docker_mysql_dag',
         default_args=default_args,
         schedule_interval='@daily',
         ) as dag:

    run_sql_insert_container = KubernetesPodOperator(
        namespace='airflow',
        image='foudazdocker/mysql:1.0',  
        cmds=["python", "./SQL_insert.py"],  
        name="run-sql-insert-container",
        task_id="run_sql_insert_container_task",
        get_logs=True
    )
