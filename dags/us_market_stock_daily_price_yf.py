import os
import pendulum
import sys
sys.path.append('/opt/airflow/')

from utils.market_open_status import GetUSAMarketOpenStatus
from project.us_market.stock_daily_price_yf import StockDataProcessor

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

"""
1. SPY ETF 를 기준으로 하기 때문에 1993-01-01 을 시작날짜로 설정
- S&P500 INDEX 를 기준으로 할 경우 1929년 부터 가능 - 그러나 그것은 다른 영역에서 처리 예정
2. 2023-09 기준 약 500 ~ 600개 종목 정보를 가져오나, 향후 늘릴 계획. 방법은 준비 다 해둠
3. 데이터 가져온 후 지정된 S3 bucket 에 들어감
"""

with DAG(
    dag_id="us_market_yf_daily_price",
    start_date=pendulum.datetime(2023, 11, 6, 9),
    schedule_interval='0 0 * * 2-6',  # 한국 시간 아침 9시
    # default_args=default_args,
    catchup=False
) as dag:
    stock_data_processor = StockDataProcessor()

    def get_pdt_date_from_utc_time(pendulum_utc_datetime):
        # utc_time : pendulum.datetime - kwargs["data_interval_end"] or kwargs["data_interval_start"]
        pdt_time = pendulum_utc_datetime.in_timezone('America/Los_Angeles')
        pdt_date = pdt_time.to_date_string().replace('-', '')  # "%Y%m%d"

        return pdt_date

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
        open_yn_result = kwargs["ti"].xcom_pull(
            key="open_yn_result",
            task_ids="get_market_open_status"
        )
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
        data_directory_path = stock_data_processor.get_data_directory_path()

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
                        replace=True
                    )

        stock_data_processor.remove_files_in_directory(data_directory_path)

    def download_stock_csv_data(**kwargs):
        data_interval_end = kwargs["data_interval_end"]
        target_date = get_pdt_date_from_utc_time(data_interval_end)
        stock_data_processor.get_stock_df_csv_files(target_date)


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

    download_stock_data_csv_task = PythonOperator(
        task_id="download_stock_data_csv",
        python_callable=download_stock_csv_data,
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
        token=stock_data_processor.get_slack_channel_info()['token'],
        channel=stock_data_processor.get_slack_channel_info()['channel'],
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    get_market_open_status_task >> check_open_status_task
    check_open_status_task >> market_closed_task >> done_task
    check_open_status_task >> market_opened_task
    market_opened_task >> download_stock_data_csv_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task
