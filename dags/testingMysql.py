from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

default_args = {
    'owner': 'Fouda',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
# Define the DAG
dag = DAG(
    'MysqlServerConnection',
    default_args=default_args,
    description='Testing SQL server connection',
    schedule_interval=None
)


#task1 = EmptyOperator(task_id='task1')

# Task to create the table
create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_id',  # Replace with your connection ID
    sql="""
    CREATE TABLE Country (
        country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        name TEXT,
        continent TEXT
    );
    """,
    dag=dag,
)



create_table

