import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Load your existing code functions
from automation import get_last_rowid, get_latest_records, insert_records

# Define the DAG
with DAG(
    dag_id="daily_data_sync",
    start_date=datetime.datetime(2024, 3, 11),
    schedule_interval="*/2 * * * *",  # Run every 2 minutes
) as dag:
    # Task to get the last rowid
    get_last_rowid_task = PythonOperator(
        task_id="get_last_rowid",
        python_callable=get_last_rowid
    )

    # Task to get latest records
    get_latest_records_task = PythonOperator(
        task_id="get_latest_records",
        python_callable=get_latest_records,
        op_kwargs={"rowid": "{{ task_instance.xcom_pull(task_ids='get_last_rowid') }}"}
    )

    # Task to insert records
    insert_records_task = PythonOperator(
        task_id="insert_records",
        python_callable=insert_records,
        op_kwargs={"records": "{{ task_instance.xcom_pull(task_ids='get_latest_records') }}"}
    )

# Adjust task dependencies
get_last_rowid_task >> get_latest_records_task >> insert_records_task
