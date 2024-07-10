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



create_table_mysql = KubernetesPodOperator(
    namespace='airflow',
    image='foudazdocker/createtable:1.0', 
    cmds=["python", "/usr/src/app/create_table.py"],
    name="create_table_mysql",
    task_id="create_table_mysql",
    get_logs=True,
    dag=dag
)

insert_to_mysql = KubernetesPodOperator(
    namespace='airflow',
    image='foudazdocker/insert_to_mysql:2.0', 
    cmds=["python", "/usr/src/app/insert_to_mysql.py"],
    name="insert_to_mysql",
    task_id="insert_to_mysql",
    get_logs=True,
    dag=dag
)


create_table_mysql >> insert_to_mysql
