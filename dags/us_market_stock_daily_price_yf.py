import pendulum
from datetime import timedelta

from utils.market_open_status import USAMarketOpenStatus
from utils.slack_alert import SlackAlert
from utils.utility_functions import UtilityFunctions
from project.us_market.stock_ticker_info import StockDataHandler
from project.us_market.stock_daily_price_yf import StockDataProcessor

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

slack_alert = SlackAlert()
stock_data_handler = StockDataHandler()
stock_data_processor = StockDataProcessor()


default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': True,
    'on_failure_callback': slack_alert.create_failure_alert,
    # 'on_success_callback': slack_alert.create_success_alert,
}

with DAG(
    dag_id="us_market_yf_daily_price",
    start_date=pendulum.datetime(2023, 12, 24, 19, tz="America/New_York"),
    schedule_interval='0 21 * * 1-5 ',  # 한국 시간 아침 10시 or 11시 (dependency : summer time)
    default_args=default_args,
    catchup=False,
    tags=['us_market', 'stock']
) as dag:
    @task(task_id="get_run_date_task")
    def get_run_date(**context):
        run_date = context["data_interval_end"].in_timezone("America/New_York").format('YYYYMMDD')  # UTC -> EST

        return run_date

    def verify_market_open_status(**context):
        target_date = context['ti'].xcom_pull(task_ids="get_run_date_task")
        open_yn_result = USAMarketOpenStatus.get_us_market_open_status(target_date)

        if open_yn_result == "Y":
            return "market_opened_task"
        else:
            return "market_closed_task"

    @task(task_id='run_sp500_task')
    def run_sp500_task(**context):
        run_dt = context['ti'].xcom_pull(task_ids="get_run_date_task")

        sp500_df = stock_data_handler.get_sp500_data()
        sp500_ticker_list = sp500_df['ticker'].tolist()

        sp500_stock_df = stock_data_processor.generate_stock_dataframe(
            run_dt, sp500_ticker_list
        )

        return sp500_stock_df

    @task(task_id='run_nasdaq100_task')
    def run_nasdaq100_task(**context):
        run_dt = context['ti'].xcom_pull(task_ids="get_run_date_task")

        nasdaq100_df = stock_data_handler.get_nasdaq100_data()
        nasdaq100_ticker_list = nasdaq100_df['ticker'].tolist()

        nasdaq100_stock_df = stock_data_processor.generate_stock_dataframe(
            run_dt, nasdaq100_ticker_list
        )

        return nasdaq100_stock_df

    @task(task_id='run_dow30_task')
    def run_dow30_task(**context):
        run_dt = context['ti'].xcom_pull(task_ids="get_run_date_task")

        dow30_df = stock_data_handler.get_dow30_data()
        dow30_ticker_list = dow30_df['ticker'].tolist()

        dow30_df = stock_data_processor.generate_stock_dataframe(
            run_dt, dow30_ticker_list
        )

        return dow30_df

    @task(task_id='run_sp400_task')
    def run_sp400_task(**context):
        run_dt = context['ti'].xcom_pull(task_ids="get_run_date_task")

        sp400_df = stock_data_handler.get_sp400_data()
        sp400_ticker_list = sp400_df['ticker'].tolist()

        sp400_stock_df = stock_data_processor.generate_stock_dataframe(
            run_dt, sp400_ticker_list
        )

        return sp400_stock_df

    @task(task_id='run_sp600_task')
    def run_sp600_task(**context):
        run_dt = context['ti'].xcom_pull(task_ids="get_run_date_task")

        sp600_df = stock_data_handler.get_sp600_data()
        sp600_ticker_list = sp600_df['ticker'].tolist()

        sp600_stock_df = stock_data_processor.generate_stock_dataframe(
            run_dt, sp600_ticker_list
        )

        return sp600_stock_df

    @task(task_id='run_various_stock_task')
    def run_various_stock_task(**context):
        run_dt = context['ti'].xcom_pull(task_ids="get_run_date_task")

        various_df = stock_data_handler.get_various_stock_data()
        various_stock_ticker_list = various_df['ticker'].tolist()

        various_stock_df = stock_data_processor.generate_stock_dataframe(
            run_dt, various_stock_ticker_list
        )

        return various_stock_df

    # TODO : 합치는 코드, 그리고 지수 종목 인지 표시, 함수명 적절 하게 변경 필요
    def combine_stock_data_df(**context):
        ti = context['ti']
        sp500_df = ti.xcom_pull(task_ids='run_sp500_task')
        nasdaq100_df = ti.xcom_pull(task_ids='run_nasdaq100_task')
        dow30_df = ti.xcom_pull(task_ids='run_dow30_task')
        sp400_df = ti.xcom_pull(task_ids='run_sp400_task')
        sp600_df = ti.xcom_pull(task_ids='run_sp600_task')
        various_stock_df = ti.xcom_pull(task_ids='run_various_stock_task')

        dfs = [sp500_df, nasdaq100_df, dow30_df, sp400_df, sp600_df, various_stock_df]
        combine_df = stock_data_handler.combine_and_process_stock_data(dfs)

        return combine_df

    def save_df_to_csv(save_dir_name, **context):
        combine_df = context['ti'].xcom_pull(task_ids="combine_stock_data_df")
        stock_data_processor.save_dataframe_to_csv(combine_df, save_dir_name)

    def upload_csv_to_s3_bucket(csv_dir_name):
        from airflow.models import Variable
        aws_json = Variable.get(key="aws", deserialize_json=True)
        # bucket_name = aws_json["aws_s3_bucket"]["yf_bucket"]
        bucket_name = aws_json["aws_s3_bucket"]["test_bucket"]  # test

        data_directory_path = UtilityFunctions.make_data_directory_path(csv_dir_name)
        UtilityFunctions.upload_file_to_s3_bucket(data_directory_path, bucket_name)
        UtilityFunctions.remove_files_in_directory(data_directory_path)

    verify_market_open_status_task = BranchPythonOperator(
        task_id="verify_market_open_status",
        python_callable=verify_market_open_status,
        provide_context=True
    )

    combine_stock_data_df_task = PythonOperator(
        task_id="combine_stock_data_df",
        python_callable=combine_stock_data_df,
        provide_context=True
    )

    save_df_to_csv_task = PythonOperator(
        task_id="save_df_to_csv",
        python_callable=save_df_to_csv,
        provide_context=True,
        op_args=['yf_total_price_aggregation']
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_opened_task = EmptyOperator(task_id="market_opened_task")

    alert_market_close_task = PythonOperator(
        task_id="alert_market_closed_task",
        python_callable=slack_alert.create_market_close_alert,
        provide_context=True
    )

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket,
        op_args=['yf_total_price_aggregation']
    )

    done_task = EmptyOperator(
        task_id="done_task",
        trigger_rule="none_failed",
        on_success_callback=[slack_alert.create_success_alert]
    )

    get_run_date_task = get_run_date()
    run_sp500 = run_sp500_task()
    run_nasdaq100 = run_nasdaq100_task()
    run_dow30 = run_dow30_task()
    run_sp400 = run_sp400_task()
    run_sp600 = run_sp600_task()
    run_various_stock = run_various_stock_task()

    get_run_date_task >> verify_market_open_status_task >> market_closed_task >> alert_market_close_task >> done_task
    verify_market_open_status_task >> market_opened_task
    market_opened_task >> [run_sp500, run_nasdaq100, run_dow30, run_sp400, run_sp600, run_various_stock] >> combine_stock_data_df_task
    combine_stock_data_df_task >> save_df_to_csv_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task
