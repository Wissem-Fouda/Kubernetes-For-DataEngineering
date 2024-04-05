from airflow import DAG
from datetime import datetime
from pandas import Series

# Configure DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 5),  # Today's date
    'retries': 1,
    
}

with DAG('simple_pandas_dag', default_args=default_args, schedule_interval=None) as dag:

    def create_series_task():
        # Create a simple pandas Series
        data = Series([1, 2, 3, 4], index=['a', 'b', 'c', 'd'])
        print(f"Created pandas Series:\n {data}")

    # Define task
    create_series = PythonOperator(
        task_id='create_series',
        python_callable=create_series_task,
    )
