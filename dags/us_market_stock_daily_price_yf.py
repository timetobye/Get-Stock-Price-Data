import os
import pendulum
import sys
sys.path.append('/opt/airflow/')

from datetime import timedelta

from utils.market_open_status import USAMarketOpenStatus
from utils.slack_alert import SlackAlert
from utils.utility_functions import UtilityFunctions
from project.us_market.stock_daily_price_yf import StockDataProcessor

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

"""
1. SPY ETF 를 기준으로 하기 때문에 1993-01-01 을 시작날짜로 설정
- S&P500 INDEX 를 기준으로 할 경우 1929년 부터 가능 - 그러나 그것은 다른 영역에서 처리 예정
2. 2023-09 기준 약 500 ~ 600개 종목 정보를 가져오나, 향후 늘릴 계획. 방법은 준비 다 해둠
3. 데이터 가져온 후 지정된 S3 bucket 에 들어감
"""

slack_alert = SlackAlert()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_alert.create_failure_alert,
    # 'on_success_callback': slack_alert.create_success_alert,
}

with DAG(
    dag_id="us_market_yf_daily_price",
    start_date=pendulum.datetime(2023, 12, 24, 19, tz="America/New_York"),
    # schedule_interval='0 0 * * 2-6',  # 한국 시간 아침 9시
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

    def download_and_save_stock_data(**context):
        data_interval_end = context["data_interval_end"]
        target_date = UtilityFunctions.get_est_date_from_utc_time(data_interval_end)

        stock_data_processor = StockDataProcessor()
        result_df = stock_data_processor.generate_stock_dataframe(target_date)
        stock_data_processor.save_dataframe_to_csv(result_df)

    def upload_csv_to_s3_bucket():
        from airflow.models import Variable
        aws_json = Variable.get(key="aws", deserialize_json=True)
        bucket_name = aws_json["aws_s3_bucket"]["yf_bucket"]
        # bucket_name = aws_json["aws_s3_bucket"]["test_bucket"]  # test

        data_directory_path = UtilityFunctions.make_data_directory_path()
        UtilityFunctions.upload_file_to_s3_bucket(data_directory_path, bucket_name)
        UtilityFunctions.remove_files_in_directory(data_directory_path)

    verify_market_open_status_task = BranchPythonOperator(
        task_id="verify_market_open_status",
        python_callable=verify_market_open_status,
        provide_context=True
    )

    download_and_save_stock_data_task = PythonOperator(
        task_id="download_and_save_stock_data",
        python_callable=download_and_save_stock_data,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_opened_task = EmptyOperator(task_id="market_opened_task")

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    verify_market_open_status_task >> market_opened_task
    verify_market_open_status_task >> market_closed_task >> done_task
    market_opened_task >> download_and_save_stock_data_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task
