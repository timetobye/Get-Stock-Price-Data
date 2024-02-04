import pendulum

from project.us_market.stock_maximum_drawdown import StockMDDProcessor
from utils.slack_alert import SlackAlert
from utils.utility_functions import UtilityFunctions
from project.us_market.stock_ticker_info import StockDataHandler
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

slack_alert = SlackAlert()
stock_data_handler = StockDataHandler()
stock_mdd = StockMDDProcessor()

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
    @task(task_id='run_sp500_task')
    def run_sp500_task():
        sp500_df = stock_data_handler.get_sp500_data()
        sp500_ticker_list = sp500_df['ticker'].tolist()
        stock_mdd.make_stock_mdd_csv_files(sp500_ticker_list)

    @task(task_id='run_nasdaq100_task')
    def run_nasdaq100_task():
        nasdaq100_df = stock_data_handler.get_nasdaq100_data()
        nasdaq100_ticker_list = nasdaq100_df['ticker'].tolist()
        stock_mdd.make_stock_mdd_csv_files(nasdaq100_ticker_list)

    @task(task_id='run_dow30_task')
    def run_dow30_task():
        dow30_df = stock_data_handler.get_dow30_data()
        dow30_ticker_list = dow30_df['ticker'].tolist()
        stock_mdd.make_stock_mdd_csv_files(dow30_ticker_list)

    @task(task_id='run_sp400_task')
    def run_sp400_task():
        sp400_df = stock_data_handler.get_sp400_data()
        sp400_ticker_list = sp400_df['ticker'].tolist()
        stock_mdd.make_stock_mdd_csv_files(sp400_ticker_list)

    @task(task_id='run_sp600_task')
    def run_sp600_task():
        sp600_df = stock_data_handler.get_sp600_data()
        sp600_ticker_list = sp600_df['ticker'].tolist()
        stock_mdd.make_stock_mdd_csv_files(sp600_ticker_list)

    @task(task_id='run_various_stock_task')
    def run_various_stock_task():
        various_df = stock_data_handler.get_various_stock_data()
        various_stock_ticker_list = various_df['ticker'].tolist()
        stock_mdd.make_stock_mdd_csv_files(various_stock_ticker_list)

    def upload_csv_to_s3_bucket(csv_dir_name):
        from airflow.models import Variable
        aws_json = Variable.get(key="aws", deserialize_json=True)
        bucket_name = aws_json["aws_s3_bucket"]["mdd_bucket"]
        # bucket_name = aws_json["aws_s3_bucket"]["test_bucket"]

        data_directory_path = UtilityFunctions.make_data_directory_path(csv_dir_name)
        UtilityFunctions.upload_file_to_s3_bucket(data_directory_path, bucket_name)
        UtilityFunctions.remove_files_in_directory(data_directory_path)

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "Start us_market_yf_maximum_drawdown task!"',
    )

    run_sp500 = run_sp500_task()
    run_nasdaq100 = run_nasdaq100_task()
    run_dow30 = run_dow30_task()
    run_sp400 = run_sp400_task()
    run_sp600 = run_sp600_task()
    run_various_stock = run_various_stock_task()

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket,
        op_args=['yf_individual_stock_mdd']
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    start_task >> [run_sp500, run_nasdaq100, run_dow30, run_sp400, run_sp600, run_various_stock] >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task