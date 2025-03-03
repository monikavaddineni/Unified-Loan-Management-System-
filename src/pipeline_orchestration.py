from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 1, 1),
}

# Define the DAG
with DAG(
    'lms_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline for LMS data',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Step 1: Data Ingestion
    load_old_data = PythonOperator(
        task_id='load_old_lms_data',
        python_callable=load_old_lms_data
    )

    load_new_data = PythonOperator(
        task_id='load_new_lms_data',
        python_callable=load_new_lms_data
    )

    # Step 2: Data Transformation
    transform_old_data = PythonOperator(
        task_id='transform_old_lms_data',
        python_callable=clean_and_transform_old_lms_data
    )

    transform_new_data = PythonOperator(
        task_id='transform_new_lms_data',
        python_callable=clean_and_transform_new_lms_data
    )

    # Step 3: Data Merging
    merge_data_task = PythonOperator(
        task_id='merge_data',
        python_callable=merge_lms_data
    )

    # Define task dependencies
    load_old_data >> load_new_data >> transform_old_data >> transform_new_data >> merge_data_task

