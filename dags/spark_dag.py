from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from spark_aggregation import spark_aggregation

# Создание DAG
with DAG('spark_log_data_aggregation', 
         start_date=datetime(2023, 1, 1), 
         schedule_interval='0 7 * * *',  # Ежедневно в 7 утра
         catchup=False) as dag:
    
    aggregation_task = PythonOperator(
        task_id='spark_aggregation_task',
        python_callable=spark_aggregation,
        op_kwargs={'target_date': '{{ ds }}'},  # Airflow предоставляет переменную шаблона {{ ds }} для дат
    )
