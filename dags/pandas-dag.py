

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
        print(f"Created pandas Series:\n {data}")

    # Define task to create Series
    create_series = PythonOperator(
        task_id='create_series',
        python_callable=create_series_task,
    )
    def second_task():
        # Creating a dictionary with data
        data2 = {'Name': ['John', 'Anna', 'Peter'], 'Age': [28, 24, 22]}
        # Converting the dictionary into a DataFrame
        df = pandas.DataFrame(data2)
        # Printing the DataFrame
        print(df)

    # Define
    create_second = PythonOperator(
        task_id='create_second',
        python_callable=create_series_task,
    )

    
