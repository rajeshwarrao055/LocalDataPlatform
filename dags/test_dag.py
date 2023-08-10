from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
from minio.error import S3Error

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 10),  # Adjust the start date
    'retries': 1,
}

dag = DAG(
    'minio_copy_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set the scheduling interval as needed
    catchup=False,
)

minio_config = {
    'endpoint': 'localhost:9001',
    'access_key': 'AOAbxzELSriTadwoIwBN',
    'secret_key': 'LnTM96i1EImstHWZgRn5F4qofJsO1c5SFT4r1unK',
    'secure': False,  # Adjust to True if using HTTPS
}


def copy_within_minio():
    source_file = 'test_bucket/some_file.csv'
    dest_file = 'test_bucket/copied_file.csv'

    client = Minio(
        endpoint=minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure'],
    )

    try:
        print(f"Trying to copy File '{source_file}' to '{dest_file}' within MinIO bucket.")
        client.copy_object(dest_file, source_file)
        print("Success !")
    except S3Error as e:
        print(f"Error copying file: {e}")

copy_task = PythonOperator(
    task_id='copy_task',
    python_callable=copy_within_minio,
    dag=dag,
)

# Define the execution order
copy_task
