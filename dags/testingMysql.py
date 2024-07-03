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
    'Mysql server connection',
    default_args=default_args,
    description='Testing SQL server connection',
    schedule_interval=None
)


task1 = EmptyOperator(task_id="task1")

# Task to create the table
create_table_task = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_id',  # Replace with your connection ID
    sql="""
    CREATE TABLE IF NOT EXISTS my_table (  
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(255) NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """,
)



task1 >> create_table_task

