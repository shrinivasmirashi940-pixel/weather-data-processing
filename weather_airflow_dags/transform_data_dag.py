from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 9, 6),
}

# Define the DAG
with DAG(
    dag_id="transformed_weather_data_to_bq",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Generate a unique batch ID using UUID
    batch_id = f"weather-data-batch-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

    # Submit PySpark job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://weather-data-gds-new/script/weather_data_processing.py",
            "python_file_uris": [],
            "jar_file_uris": [],
            "args": []
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": "286445309596-compute@developer.gserviceaccount.com",
                "network_uri": "projects/project-2eef9167-38f3-4223-ac1/global/networks/default",
                "subnetwork_uri": "projects/project-2eef9167-38f3-4223-ac1/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id="spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=batch_id,
        project_id="project-2eef9167-38f3-4223-ac1",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Task Dependencies
    pyspark_task