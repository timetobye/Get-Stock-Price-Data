import pendulum

from project.us_market.stock_maximum_drawdown import upload_stock_mdd_to_s3
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

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_alert.create_failure_alert,
    # 'on_success_callback': slack_alert.create_success_alert,
}

with DAG(
    dag_id="us_market_yf_maximum_drawdown",
    start_date=pendulum.datetime(2023, 12, 24, 19, tz="America/New_York"),
    schedule_interval='0 1 * * 7',
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_args,
    on_success_callback=slack_alert.create_success_alert,
    catchup=False,
    tags=['us_market', 'stock']
) as dag:
    @task(task_id='run_sp500_task')
    def run_sp500_task(**kwargs):
        sp500_df = stock_data_handler.get_sp500_data()
        sp500_ticker_list = sp500_df['ticker'].tolist()
        upload_stock_mdd_to_s3(sp500_ticker_list, **kwargs)

    @task(task_id='run_nasdaq100_task')
    def run_nasdaq100_task(**kwargs):
        nasdaq100_df = stock_data_handler.get_nasdaq100_data()
        nasdaq100_ticker_list = nasdaq100_df['ticker'].tolist()
        upload_stock_mdd_to_s3(nasdaq100_ticker_list, **kwargs)

    @task(task_id='run_dow30_task')
    def run_dow30_task(**kwargs):
        dow30_df = stock_data_handler.get_dow30_data()
        dow30_ticker_list = dow30_df['ticker'].tolist()
        upload_stock_mdd_to_s3(dow30_ticker_list, **kwargs)

    @task(task_id='run_sp400_task')
    def run_sp400_task(**kwargs):
        sp400_df = stock_data_handler.get_sp400_data()
        sp400_ticker_list = sp400_df['ticker'].tolist()
        upload_stock_mdd_to_s3(sp400_ticker_list, **kwargs)

    @task(task_id='run_sp600_task')
    def run_sp600_task(**kwargs):
        sp600_df = stock_data_handler.get_sp600_data()
        sp600_ticker_list = sp600_df['ticker'].tolist()
        upload_stock_mdd_to_s3(sp600_ticker_list, **kwargs)

    @task(task_id='run_various_stock_task')
    def run_various_stock_task(**kwargs):
        various_df = stock_data_handler.get_various_stock_data()
        various_stock_ticker_list = various_df['ticker'].tolist()
        upload_stock_mdd_to_s3(various_stock_ticker_list, **kwargs)


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

    done_task = EmptyOperator(
        task_id="done_task",
        trigger_rule="none_failed",
        on_success_callback=[slack_alert.create_success_alert]
    )

    start_task >> [run_sp500, run_nasdaq100, run_dow30, run_sp400, run_sp600, run_various_stock] >> done_task