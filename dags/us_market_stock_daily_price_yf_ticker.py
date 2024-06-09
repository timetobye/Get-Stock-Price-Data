import pendulum

from datetime import timedelta

from utils.market_open_status import USAMarketOpenStatus
from utils.slack_alert import SlackAlert
from project.us_market.stock_daily_price_yf_ticker import upload_stock_data_to_s3
from project.us_market.stock_ticker_info import StockDataHandler

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

slack_alert = SlackAlert()
stock_data_handler = StockDataHandler()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': True,
    'on_failure_callback': slack_alert.create_failure_alert,
    # 'on_success_callback': slack_alert.create_success_alert,
}

with DAG(
    dag_id="us_market_yf_ticker_daily_price",
    start_date=pendulum.datetime(2023, 12, 24, 19, tz="America/New_York"),
    schedule_interval='20 21 * * 1-5',  # 한국 시간 아침 10시 or 11시 (dependency : summer time)
    dagrun_timeout=timedelta(minutes=20),
    # schedule_interval=None,  # trigger_dag_run
    default_args=default_args,
    # on_success_callback=slack_alert.create_success_alert,
    catchup=False,
    tags=['us_market', 'stock']
) as dag:
    def verify_market_open_status(**kwargs):
        target_date = kwargs["data_interval_end"].in_timezone("America/New_York").format('YYYYMMDD')
        open_yn_result = USAMarketOpenStatus.get_us_market_open_status(target_date)

        if open_yn_result:
            return "market_opened_task"
        else:
            return "market_closed_task"


    @task(task_id='run_sp500_task')
    def run_sp500_task(**kwargs):
        sp500_df = stock_data_handler.get_sp500_data()
        sp500_ticker_list = sp500_df['ticker'].tolist()
        upload_stock_data_to_s3(sp500_ticker_list, **kwargs)


    @task(task_id='run_nasdaq100_task')
    def run_nasdaq100_task(**kwargs):
        nasdaq100_df = stock_data_handler.get_nasdaq100_data()
        nasdaq100_ticker_list = nasdaq100_df['ticker'].tolist()
        upload_stock_data_to_s3(nasdaq100_ticker_list, **kwargs)


    @task(task_id='run_dow30_task')
    def run_dow30_task(**kwargs):
        dow30_df = stock_data_handler.get_dow30_data()
        dow30_ticker_list = dow30_df['ticker'].tolist()
        upload_stock_data_to_s3(dow30_ticker_list, **kwargs)


    @task(task_id='run_sp400_task')
    def run_sp400_task(**kwargs):
        sp400_df = stock_data_handler.get_sp400_data()
        sp400_ticker_list = sp400_df['ticker'].tolist()
        upload_stock_data_to_s3(sp400_ticker_list, **kwargs)


    @task(task_id='run_sp600_task')
    def run_sp600_task(**kwargs):
        sp600_df = stock_data_handler.get_sp600_data()
        sp600_ticker_list = sp600_df['ticker'].tolist()
        upload_stock_data_to_s3(sp600_ticker_list, **kwargs)


    @task(task_id='run_various_stock_task')
    def run_various_stock_task(**kwargs):
        various_df = stock_data_handler.get_various_stock_data()
        various_stock_ticker_list = various_df['ticker'].tolist()
        upload_stock_data_to_s3(various_stock_ticker_list, **kwargs)


    verify_market_open_status_task = BranchPythonOperator(
        task_id="verify_market_open_status",
        python_callable=verify_market_open_status,
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_opened_task = EmptyOperator(task_id="market_opened_task")

    alert_market_close_task = PythonOperator(
        task_id="alert_market_closed_task",
        python_callable=slack_alert.create_market_close_alert,
    )

    done_task = EmptyOperator(
        task_id="done_task",
        trigger_rule="none_failed",
        on_success_callback=[slack_alert.create_success_alert]
    )

    run_sp500 = run_sp500_task()
    run_nasdaq100 = run_nasdaq100_task()
    run_dow30 = run_dow30_task()
    run_sp400 = run_sp400_task()
    run_sp600 = run_sp600_task()
    run_various_stock = run_various_stock_task()

    verify_market_open_status_task >> market_closed_task >> alert_market_close_task >> done_task
    verify_market_open_status_task >> market_opened_task
    market_opened_task >> [run_sp500, run_nasdaq100, run_dow30, run_sp400, run_sp600, run_various_stock] >> done_task
