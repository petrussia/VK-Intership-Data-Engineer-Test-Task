from support_funcs import parse_arguments, calculate_date_range, read_log_data, aggregate_actions, save_aggregated_data
import sys

def main():
    target_date = parse_arguments()
    start_date, end_date = calculate_date_range(target_date)
    log_data = read_log_data(start_date, end_date)
    if log_data.empty:
        print("Нет данных для агрегации за указанный период.")
        sys.exit(1)
    aggregated_data = aggregate_actions(log_data)
    if not aggregated_data.empty:
        save_aggregated_data(aggregated_data, target_date)
    else:
        print("После агрегации данные отсутствуют.")


if __name__ == '__main__':
    main()
