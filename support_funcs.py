import os
import pandas as pd
from datetime import datetime, timedelta
import argparse
import sys


def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Weekly action log aggregation.')
    parser.add_argument('date', type=str, help='Specify the target date (format YYYY-mm-dd)')
    args = parser.parse_args()
    return args.date

def calculate_date_range(target_date):
    """Возвращает диапазон дат для недельного интервала после проверки корректности вводимой пользователем даты"""
    try:
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError as e:
        print(f"У вас ОШИБКА!: Неправильный формат даты '{target_date}'. Ожидается формат YYYY-mm-dd.")
        sys.exit(1)
    start_date = target_date_obj - timedelta(days=7)
    end_date = target_date_obj - timedelta(days=1)
    return start_date, end_date


def verify_file_existence(file_path):
    """Проверяет наличие/отсуствие файла"""
    return os.path.exists(file_path)


def read_log_data(start_date, end_date):
    """Чтение данных за указанный период из существующих файлов"""
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    data_frames = []

    for date in date_list:
        file_name = f"{date.strftime('%Y-%m-%d')}.csv"
        file_path = os.path.join('input', file_name)

        if verify_file_existence(file_path):
            try:
                df = pd.read_csv(file_path, header=None, names=['email', 'action', 'timestamp'])
                data_frames.append(df)
            except pd.errors.EmptyDataError:
                print(f"Ошибка: Файл {file_name} пустой. Пропускаем.")
            except Exception as e:
                print(f"Ошибка: Не удалось прочитать {file_name}. {str(e)}")
        else:
            print(f"Файл {file_name} не найден. Пропускаем.")

    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()


def aggregate_actions(df):
    """Агрегация данных по пользователям и действиям"""
    if df.empty:
        print("Нет данных для обработки.")
        return pd.DataFrame(columns=['email', 'create_count', 'read_count', 'update_count', 'delete_count'])

    action_counts = df.groupby(['email', 'action']).size().unstack(fill_value=0).reset_index()
    action_counts.columns = ['email'] + [f"{action.lower()}_count" for action in action_counts.columns if action != 'email']

    for action in ['create_count', 'read_count', 'update_count', 'delete_count']:
        if action not in action_counts.columns:
            action_counts[action] = 0
    print(action_counts)
    return action_counts


def save_aggregated_data(df, target_date):
    """Сохранение агрегированных данных в csv файл"""
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    output_directory = 'output'
    os.makedirs(output_directory, exist_ok=True)
    output_file = os.path.join(output_directory, f"{target_date_obj.strftime('%Y-%m-%d')}.csv")

    try:
        df.to_csv(output_file, index=False)
        print(f"Агрегированные данные успешно сохранены в {output_file}")
    except Exception as e:
        print(f"У вас ОШИБКА!: Не удалось сохранить данные в {output_file}. {str(e)}")
