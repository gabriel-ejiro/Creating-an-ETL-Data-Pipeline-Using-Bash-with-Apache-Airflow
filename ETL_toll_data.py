from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
    
# DAG arguments
default_args = {
    owner = 'Gabriel Ejiro',
    start_date = days_ago(0),
    email = ['ejirogabriell2019@gmail.com'],
    email_on_failure = True,
    email_on_retry = True,
    retries = 1,
    retry_delay = timedelta(minutes=5),
}

# DAG definition
dag = DAG(
      dag_id='ETL_toll_data',
      default_args=default_args,
      description = 'Apache Airflow Final Assignment',
      schedule_interval=timedelta(days=1),
)

# Task: extract_transform_load
extract_transform_load_task = BashOperator(
    task_id='extract_transform_load',
    bash_command='/home/project/airflow/dags/Extract_Transform_data.sh',
    dag=dag
)

# submit the DAG
airflow dags trigger ETL_toll_data

