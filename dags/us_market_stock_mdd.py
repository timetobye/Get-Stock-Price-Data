import os
import pendulum
import sys
sys.path.append('/opt/airflow/')

from project.us_market.stock_maximum_drawdown import StockMDDProcessor
from utils.slack_alert import SlackAlert
from utils.utility_functions import UtilityFunctions
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

slack_alert = SlackAlert()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_alert.create_failure_alert,
    # 'on_success_callback': slack_alert.create_success_alert,
}

with DAG(
    dag_id="us_market_yf_maximum_drawdown",
    start_date=pendulum.datetime(2023, 12, 24, 19, tz="America/New_York"),
    schedule_interval='0 1 * * 7',  # 매주 일요일 오전 10시 - KST
    default_args=default_args,
    on_success_callback=slack_alert.create_success_alert,
    catchup=False
) as dag:
    def get_stock_ticker_df_csv_files():
        stock_mdd = StockMDDProcessor()
        stock_mdd.make_stock_mdd_csv_files()

    def upload_csv_to_s3_bucket():
        from airflow.models import Variable
        aws_json = Variable.get(key="aws", deserialize_json=True)
        bucket_name = aws_json["aws_s3_bucket"]["mdd_bucket"]
        # bucket_name = aws_json["aws_s3_bucket"]["test_bucket"]

        data_directory_path = UtilityFunctions.make_data_directory_path()
        UtilityFunctions.upload_file_to_s3_bucket(data_directory_path, bucket_name)
        UtilityFunctions.remove_files_in_directory(data_directory_path)

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "Start us_market_yf_maximum_drawdown task!"',
    )

    get_stock_ticker_df_csv_files_task = PythonOperator(
        task_id="get_stock_ticker_df_csv_files",
        python_callable=get_stock_ticker_df_csv_files,
        provide_context=True
    )

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    start_task >> get_stock_ticker_df_csv_files_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task