

from airflow import DAG
from datetime import datetime
from pandas import Series, DataFrame
from airflow.operators.python import PythonOperator

# Configure DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 5),  # Today's date
    'retries': 1,
    
}

with DAG('pandas_twice_dag', default_args=default_args, schedule_interval=None) as dag:

    def create_series_task():
        # Create a pandas Series
        data = Series([1, 2, 3, 4], index=['a', 'b', 'c', 'd'])
        return data

    # Define task to create Series
    create_series = PythonOperator(
        task_id='create_series',
        python_callable=create_series_task,
    )

    def process_data_task(series):
        # Simulate receiving Series from previous task (replace with XCom)
        data = series

        # Create a DataFrame from the Series
        df = DataFrame(data.values.reshape(-1, 1), columns=['values'])
        print(f"Created DataFrame:\n {df}")

    # Define task to process Series
    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data_task,
        provide_context=True,  # Pass Series from previous task
        upstream_task_id=create_series.task_id,
    )

