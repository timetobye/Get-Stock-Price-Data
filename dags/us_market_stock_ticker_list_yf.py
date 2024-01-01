import os
import pendulum
import pandas as pd
import shutil
import sys
sys.path.append('/opt/airflow/')

from utils.market_open_status import USAMarketOpenStatus
from utils.slack_alert import SlackAlert
from utils.utility_functions import UtilityFunctions
from project.us_market.stock_ticker_info import GetTickerInfo
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

"""
코드 추가 수정 예정 - 240101
"""

slack_alert = SlackAlert()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_alert.create_failure_alert,
    # 'on_success_callback': slack_alert.create_success_alert,
}


with DAG(
    dag_id="us_market_yf_ticker_list",
    start_date=pendulum.datetime(2023, 12, 29, 19, tz="America/New_York"),
    # schedule_interval='30 23 * * 1-5',  # 한국 시간 아침 9시
    schedule_interval=None,  # trigger_dag_run
    default_args=default_args,
    on_success_callback=slack_alert.create_success_alert,
    catchup=False
) as dag:
    def verify_market_open_status(**context):
        # 로그 확인 목적으로 남김
        data_interval_end = context["data_interval_end"]  # data_interval_end 는 no-dash 제공을 안 해줘서 별도 코드 필요
        usa_data_interval_end = data_interval_end.in_timezone("America/New_York")
        target_date = usa_data_interval_end.to_date_string().replace('-', '')  # "%Y%m%d"

        print(f' context["data_interval_end"] : {context["data_interval_end"]}'
              f' context["data_interval_end"].strftime("%Y%m%d") : '
              f' {context["data_interval_end"].strftime("%Y%m%d")}'
              f' usa_data_interval_end : {usa_data_interval_end}'
              f' target_date : {target_date}'
              )

        open_yn_result = USAMarketOpenStatus.get_us_market_open_status(target_date)
        # open_yn_result = "Y"  # 테스트

        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_opened_task"

    def get_stock_info_df():
        ticker_info_inst = GetTickerInfo()
        stock_index_wiki_df, stock_ticker_list = ticker_info_inst.get_ticker_info()
        all_ticker_info_list = []

        # stock_ticker_list = ['SPY', 'QQQ', 'AAPL']  # test
        for idx, ticker in enumerate(stock_ticker_list):
            result = ticker_info_inst.get_stock_info_with_retry(ticker)
            all_ticker_info_list.append(result)

        all_ticker_info_df = pd.DataFrame.from_dict(all_ticker_info_list)
        all_ticker_info_df = all_ticker_info_df.merge(
            stock_index_wiki_df[['ticker', 's&p500', 'nasdaq100', 'dow30']],
            on='ticker',
            how='left'
        )

        return all_ticker_info_df


    def save_csv_from_stock_info_df(df, target_date):
        df['date'] = target_date

        data_directory_path = UtilityFunctions.make_data_directory_path()
        directory_path = f"{data_directory_path}{os.sep}{target_date}"
        os.makedirs(directory_path, exist_ok=True)

        all_ticker_file_name = f"{directory_path}{os.sep}" \
                               f"us_market_ticker_{target_date}_{target_date}.csv"
        df.to_csv(all_ticker_file_name, index=False)


    def fetch_and_save_stock_data(target_date):
        stock_info_df = get_stock_info_df()
        save_csv_from_stock_info_df(stock_info_df, target_date)

    def download_stock_ticker_csv_file(**context):
        data_interval_end = context["data_interval_end"]
        target_date = UtilityFunctions.get_est_date_from_utc_time(data_interval_end)
        fetch_and_save_stock_data(target_date)

    def upload_csv_to_s3_bucket():
        from airflow.models import Variable
        aws_json = Variable.get(key="aws", deserialize_json=True)
        bucket_name = aws_json["aws_s3_bucket"]["ticker_list_bucket"]
        # bucket_name = aws_json["aws_s3_bucket"]["test_bucket"]

        data_directory_path = UtilityFunctions.make_data_directory_path()
        UtilityFunctions.upload_file_to_s3_bucket(data_directory_path, bucket_name)
        UtilityFunctions.remove_files_in_directory(data_directory_path)

    verify_market_open_status_task = BranchPythonOperator(
        task_id="verify_market_open_status",
        python_callable=verify_market_open_status,
        provide_context=True
    )

    download_stock_ticker_data_csv_task = PythonOperator(
        task_id="download_stock_ticker_data_csv",
        python_callable=download_stock_ticker_csv_file,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_opened_task = EmptyOperator(task_id="market_opened_task")

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    verify_market_open_status_task >> market_closed_task >> done_task
    verify_market_open_status_task >> market_opened_task
    market_opened_task >> download_stock_ticker_data_csv_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task