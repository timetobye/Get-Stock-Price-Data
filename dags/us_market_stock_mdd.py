import os
import sys
sys.path.append('/opt/airflow/')

from utils.market_open_status import GetUSAMarketOpenStatus
from project.us_market.stock_maximum_drawdown import StockMaximumDrawdownProcessor
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator


def get_pdt_date_from_utc_time(pendulum_utc_datetime):
    # utc_time : pendulum.datetime - kwargs["data_interval_end"] or kwargs["data_interval_start"]
    pdt_time = pendulum_utc_datetime.in_timezone('America/Los_Angeles')
    pdt_date = pdt_time.to_date_string().replace('-', '')  # "%Y%m%d"

    return pdt_date


with DAG(
    dag_id="us_market_yf_maximum_drawdown",
    start_date=datetime(2023, 11, 6, 9),
    schedule_interval='0 1 * * 6',  # 한국 시간 아침 10시
    # default_args=default_args,
    catchup=False
) as dag:
    stock_mdd = StockMaximumDrawdownProcessor()
    def get_market_open_status(**kwargs):
        check_market_open_status = GetUSAMarketOpenStatus()

        data_interval_end = kwargs["data_interval_end"]
        usa_data_interval_end = data_interval_end.in_timezone('America/Los_Angeles')
        target_date = usa_data_interval_end.to_date_string().replace('-', '')  # "%Y%m%d"

        print(f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
              f' kwargs["data_interval_end"].strftime("%Y%m%d") : '
              f' {kwargs["data_interval_end"].strftime("%Y%m%d")}'
              f' usa_data_interval_end : {usa_data_interval_end}'
              f' target_date : {target_date}')

        open_yn_result = check_market_open_status.get_us_market_status(target_date)
        # open_yn_result = "Y"  # for testing
        kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)

    def check_open_status(**kwargs):
        open_yn_result = kwargs["ti"].xcom_pull(key="open_yn_result", task_ids="get_market_open_status")
        # open_yn_result = "N"

        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_opened_task"


    def make_slack_message(**kwargs):
        dag_id = kwargs['dag'].dag_id
        data_interval_end = kwargs["data_interval_end"]
        target_date = get_pdt_date_from_utc_time(data_interval_end)

        slack_message = f"""
        **결과 요약**
        - DAG 작업: {dag_id}
        - 작업 날짜 : {target_date}
        """

        task_instance = kwargs['ti']
        task_instance.xcom_push(
            key="slack_message",
            value=slack_message
        )

    def upload_csv_to_s3_bucket():
        bucket_name = 'your-bucket-name'
        data_directory_path = stock_mdd.get_data_directory_path()

        s3_hook = S3Hook('s3_conn')

        for root, dirs, files in os.walk(data_directory_path):
            for file in files:
                if file.lower().endswith('.csv'):
                    local_file_path = os.path.join(root, file)

                    s3_key = os.path.relpath(local_file_path, data_directory_path)
                    s3_hook.load_file(
                        filename=local_file_path,
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True  # mdd 여서 덮어씌워도 무방함
                    )

        stock_mdd.remove_files_in_directory(data_directory_path)

    def get_stock_ticker_df_csv_files(**kwargs):
        data_interval_end = kwargs["data_interval_end"]
        target_date = get_pdt_date_from_utc_time(data_interval_end)
        stock_mdd.make_stock_mdd_csv_files(target_date)


    get_market_open_status_task = PythonOperator(
        task_id="get_market_open_status",
        python_callable=get_market_open_status,
        provide_context=True
    )

    check_open_status_task = BranchPythonOperator(
        task_id="check_open_status",
        python_callable=check_open_status,
        provide_context=True
    )

    get_stock_ticker_df_csv_files_task = PythonOperator(
        task_id="get_stock_ticker_df_csv_files",
        python_callable=get_stock_ticker_df_csv_files,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_opened_task = EmptyOperator(task_id="market_opened_task")

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    make_slack_message_task = PythonOperator(
        task_id="make_slack_message",
        python_callable=make_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token=stock_mdd.get_slack_channel_info()['token'],
        channel=stock_mdd.get_slack_channel_info()['channel'],
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    get_market_open_status_task >> check_open_status_task
    check_open_status_task >> market_closed_task >> done_task
    check_open_status_task >> market_opened_task
    market_opened_task >> get_stock_ticker_df_csv_files_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task