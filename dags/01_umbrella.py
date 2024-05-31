from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# Define default arguments for the DAG
default_args = {
    'owner': 'Wissem Fouda',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
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
# Define task1 using KubernetesPodOperator
task1 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/tasks:1.0",
    name="task-1",
    task_id="task-1",
    get_logs=True,
    dag=dag
)
# Define task2 using KubernetesPodOperator
task2 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/demorepo:1.0",
    name="task-2",
    task_id="task-2",
    get_logs=True,
    dag=dag
)

# Define task3 using KubernetesPodOperator
task3 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/task3:1.0",
    name="task-3",
    task_id="task-3",
    get_logs=True,
    dag=dag
)

# Define task4 using KubernetesPodOperator
task4 = KubernetesPodOperator(
    namespace='airflow',
    image="foudazdocker/task4:1.0",
    name="task-4",
    task_id="task-4",
    get_logs=True,
    dag=dag
)

# Set task dependencies
task1 >> [task2, task3]
task4.set_upstream([task2, task3])
# Set task dependencies
# task1 >> task2
