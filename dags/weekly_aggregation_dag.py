from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weekly_aggregation_dag',
    default_args=default_args,
    description='Ежедневный утренний запуск spark_app.py в 7:00 для вычисления недельного агрегата',
    schedule_interval='0 7 * * *',
    catchup=False,
)

run_spark_app = SparkSubmitOperator(
    task_id='run_spark_app',
    application='/mnt/c/Users/user/Работа/Тестовые/VK/Intership/VK-Intership-Data-Engineer-Test-Task/scripts/spark_app.py',
    name='weekly_aggregation',
    application_args=['{{ ds }}'],
    dag=dag,
)
