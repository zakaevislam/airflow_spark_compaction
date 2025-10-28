from datetime import datetime
from pendulum import timezone
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator


UTC = timezone('UTC')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 1, tzinfo=UTC),
    'retries': 1,
    'depends_on_past': False,
}

with DAG(
    'spark_compaction',
    default_args=default_args,
    description='Run Spark Parquet compaction in Docker container',
    schedule='00 14 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['spark']
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    run_spark_job = DockerOperator(
        task_id='run_spark_compaction',
        image='spark-compaction-app:latest',
        container_name='spark-compaction-app',
        auto_remove=True,
        environment={
            'TARGET_SIZE_MB_PER_FILES': '1',
            'DATA_PATH': './data',
        },
        cpus=4,
        mem_limit='8g',
        command='python3 /app/spark_app.py'
    )
    end = EmptyOperator(
        task_id='end'
    )

    start >> run_spark_job >> end