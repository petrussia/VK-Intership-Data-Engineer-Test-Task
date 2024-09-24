import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import DateType
import argparse


def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Weekly action aggregation with Spark')
    parser.add_argument('date', type=str, help='Specify the target date (format YYYY-mm-dd)')
    args = parser.parse_args()
    return args.date

def calculate_date_range(target_date):
    """Возвращает диапазон дат для недельного интервала после проверки корректности вводимой пользователем даты"""
    try:
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        print(f"У вас ОШИБКА!: Неправильный формат даты '{target_date}'. Ожидается формат YYYY-mm-dd.")
        sys.exit(1)
    start_date = target_date_obj - timedelta(days=7)
    end_date = target_date_obj
    return start_date, end_date

def verify_file_existence(file_path):
    """Проверяет наличие/отсуствие файла"""
    return os.path.exists(file_path)

def read_log_data(spark, start_date, end_date):
    """Чтение данных за указанный период из существующих файлов"""
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]
    input_path = "../input"
    existing_files = []
    
    for date in date_list:
        file_name = f"{date.strftime('%Y-%m-%d')}.csv"
        file_path = os.path.join(input_path, file_name)
    
        if verify_file_existence(file_path):
            existing_files.append(file_path)
        else:
            print(f"Файл {file_name} не найден. Пропускаем.")

    if not existing_files:
        print("Нет файлов для обработки.")
        return spark.createDataFrame([], schema=['email', 'action', 'timestamp'])

    df = spark.read.csv(existing_files, header=False, inferSchema=True)
    df = df.toDF('email', 'action', 'timestamp')
    return df.withColumn("timestamp", col("timestamp").cast(DateType()))

def aggregate_actions(df):
    """Агрегация данных по пользователям и действиям"""
    if df.rdd.isEmpty():
        print("Нет данных для обработки.")
        schema = ['email', 'create_count', 'read_count', 'update_count', 'delete_count']
        return spark.createDataFrame([], schema=schema)

    actions_df = df.groupBy("email", "action").agg(count("*").alias("action_count"))
    pivot_df = actions_df.groupBy("email").pivot("action", ["CREATE", "DELETE", "READ", "UPDATE"]) \
                         .sum("action_count").fillna(0)
    # pivot_df.show()
    return pivot_df

def save_aggregated_data(result_df, target_date):
    """Сохранение агрегированных данных в csv файл"""
    output_directory = '../output'
    os.makedirs(output_directory, exist_ok=True)
    output_file_path = os.path.join(output_directory, f"{target_date}.csv")
    
    try:
        result_df.write.csv(output_file_path, header=True, mode='overwrite')
        print(f"Агрегированные данные успешно сохранены в {output_file_path}")
    except Exception as e:
        print(f"У вас ОШИБКА!: Не удалось сохранить данные в {output_file_path}. {str(e)}")

def main():
    spark = SparkSession.builder.appName("Weekly Aggregation Application").getOrCreate()
    target_date = parse_arguments()
    start_date, end_date = calculate_date_range(target_date)
    df = read_log_data(spark, start_date, end_date)
    if df.rdd.isEmpty():
        print("Нет данных для агрегации за указанный период.")
        sys.exit(1)
    aggregated_data = aggregate_actions(df)
    if aggregated_data.count() > 0:
        save_aggregated_data(aggregated_data, target_date)
    else:
        print("После агрегации данные отсутствуют.")

    spark.stop()

if __name__ == "__main__":
    main()
