default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 2),  # Today's date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='test_sqlserver_connection',
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual execution
) as dag:
    # Task to create the table
    create_table_task = MsSqlOperator(
        task_id='create_test_table',
        mssql_conn_id='your_sqlserver_conn_id',  # Replace with your connection ID
        sql="""
            CREATE TABLE IF NOT EXISTS TestTable (
                id INT PRIMARY KEY IDENTITY,
                data VARCHAR(255) NOT NULL
            );
        """,
    )
