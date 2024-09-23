from pyspark.sql import SparkSession
from support_funcs import read_log_data, aggregate_actions, save_aggregated_data, calculate_date_range

def spark_aggregation(target_date):
    start_date, end_date = calculate_date_range(target_date)
    
    # Инициализация SparkSession
    spark = SparkSession.builder \
        .appName("Log Aggregation") \
        .getOrCreate()
    
    # Использование Spark для чтения данных
    log_data = spark.read.format("csv").load(f"path/to/logs/{start_date}-{end_date}.csv")

    if log_data.rdd.isEmpty():
        print("Нет данных для агрегации за указанный период.")
        spark.stop()
        return

    # Агрегация данных с использованием Spark
    aggregated_data = aggregate_actions(log_data)

    if not aggregated_data.rdd.isEmpty():
        save_aggregated_data(aggregated_data, target_date)
    else:
        print("После агрегации данные отсутствуют.")

    spark.stop()