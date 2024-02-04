import os
import pendulum

from utils.market_open_status import USAMarketOpenStatus
from utils.slack_alert import SlackAlert
from utils.utility_functions import UtilityFunctions
from project.us_market.stock_ticker_info import StockDataHandler
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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
    dag_id="us_market_yf_ticker_list",
    start_date=pendulum.datetime(2023, 12, 29, 19, tz="America/New_York"),
    # schedule_interval='30 23 * * 1-5',  # 한국 시간 아침 9시
    schedule_interval=None,  # trigger_dag_run
    default_args=default_args,
    on_success_callback=slack_alert.create_success_alert,
    catchup=False
) as dag:
    @task(task_id="get_run_date_task")
    def get_run_date(**context):
        data_interval_end = context["data_interval_end"]  # data_interval_end 는 no-dash 제공을 안 해줘서 별도 코드 필요
        run_date = UtilityFunctions.get_est_date_from_utc_time(data_interval_end)

        return run_date

    # TODO : Operator 부분을 ShortCircuitOperator 으로 변경 예정 - True/False 기준으로 변경
    def verify_market_open_status(**context):
        target_date = context['ti'].xcom_pull(task_ids="get_run_date_task")
        print(f' target_date : {target_date}, type: {type(target_date)}')
        open_yn_result = USAMarketOpenStatus.get_us_market_open_status(target_date)
        # open_yn_result = "Y"  # 테스트

        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_opened_task"

    @task(task_id='run_sp500_task')
    def run_sp500_task():
        sp500_df = stock_data_handler.get_sp500_data()
        sp500_ticker_list = sp500_df['ticker'].tolist()
        sp500_info_df = stock_data_handler.get_stock_df_with_info(sp500_ticker_list)

        return sp500_info_df

    @task(task_id='run_nasdaq100_task')
    def run_nasdaq100_task():
        nasdaq100_df = stock_data_handler.get_nasdaq100_data()
        nasdaq100_ticker_list = nasdaq100_df['ticker'].tolist()
        nasdaq100_info_df = stock_data_handler.get_stock_df_with_info(nasdaq100_ticker_list)

        return nasdaq100_info_df

    @task(task_id='run_dow30_task')
    def run_dow30_task():
        dow30_df = stock_data_handler.get_dow30_data()
        dow30_ticker_list = dow30_df['ticker'].tolist()
        dow30_info_df = stock_data_handler.get_stock_df_with_info(dow30_ticker_list)

        return dow30_info_df

    @task(task_id='run_sp400_task')
    def run_sp400_task():
        sp400_df = stock_data_handler.get_sp400_data()
        sp400_ticker_list = sp400_df['ticker'].tolist()
        sp400_info_df = stock_data_handler.get_stock_df_with_info(sp400_ticker_list)

        return sp400_info_df

    @task(task_id='run_sp600_task')
    def run_sp600_task():
        sp600_df = stock_data_handler.get_sp600_data()
        sp600_ticker_list = sp600_df['ticker'].tolist()
        sp600_info_df = stock_data_handler.get_stock_df_with_info(sp600_ticker_list)

        return sp600_info_df

    @task(task_id='run_various_stock_task')
    def run_various_stock_task(**context):
        run_dt = context['ti'].xcom_pull(task_ids="get_run_date_task")

        various_df = stock_data_handler.get_various_stock_data()
        various_stock_ticker_list = various_df['ticker'].tolist()

        various_stock_info_df = stock_data_handler.get_stock_df_with_info(various_stock_ticker_list)

        return various_stock_info_df

    @task(task_id='combine_stock_info_df')
    def combine_stock_info_df(**context):
        ti = context['ti']  # ti : task instance

        sp500_df = ti.xcom_pull(task_ids='run_sp500_task')
        nasdaq100_df = ti.xcom_pull(task_ids='run_nasdaq100_task')
        dow30_df = ti.xcom_pull(task_ids='run_dow30_task')
        sp400_df = ti.xcom_pull(task_ids='run_sp400_task')
        sp600_df = ti.xcom_pull(task_ids='run_sp600_task')
        var_stock_df = ti.xcom_pull(task_ids='run_various_stock_task')

        dfs = [sp500_df, nasdaq100_df, dow30_df, sp400_df, sp600_df, var_stock_df]
        combined_stock_info_df = stock_data_handler.combine_and_process_stock_data(dfs)

        return combined_stock_info_df

    @task(task_id='save_df_to_csv')
    def save_df_to_csv(**context):
        combine_df = context['ti'].xcom_pull(task_ids="combine_stock_info_df")
        target_date = context['ti'].xcom_pull(task_ids="get_run_date_task")
        combine_df['date'] = target_date

        csv_dir_name = "yf_individual_stock_information"  ## 이 부분은 나주에 조정 필요
        data_directory_path = UtilityFunctions.make_data_directory_path(csv_dir_name)
        directory_path = f"{data_directory_path}{os.sep}{target_date}"
        os.makedirs(directory_path, exist_ok=True)

        all_ticker_file_name = f"{directory_path}{os.sep}" \
                               f"us_market_ticker_{target_date}_{target_date}.csv"
        combine_df.to_csv(all_ticker_file_name, index=False)

    def upload_csv_to_s3_bucket(csv_dir_name):
        from airflow.models import Variable
        aws_json = Variable.get(key="aws", deserialize_json=True)
        bucket_name = aws_json["aws_s3_bucket"]["ticker_list_bucket"]
        # bucket_name = aws_json["aws_s3_bucket"]["test_bucket"]

        data_directory_path = UtilityFunctions.make_data_directory_path(csv_dir_name)
        UtilityFunctions.upload_file_to_s3_bucket(data_directory_path, bucket_name)
        UtilityFunctions.remove_files_in_directory(data_directory_path)

    verify_market_open_status_task = BranchPythonOperator(
        task_id="verify_market_open_status",
        python_callable=verify_market_open_status,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_opened_task = EmptyOperator(task_id="market_opened_task")

    get_run_date_task = get_run_date()
    run_sp500 = run_sp500_task()
    run_nasdaq100 = run_nasdaq100_task()
    run_dow30 = run_dow30_task()
    run_sp400 = run_sp400_task()
    run_sp600 = run_sp600_task()
    run_various_stock = run_various_stock_task()
    combine_stock_df_task = combine_stock_info_df()
    save_df_to_csv_task = save_df_to_csv()

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket,
        op_args=['yf_individual_stock_information']
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    get_run_date_task >> verify_market_open_status_task >> market_closed_task >> done_task
    verify_market_open_status_task >> market_opened_task
    market_opened_task >> [run_sp500, run_nasdaq100, run_dow30, run_sp400, run_sp600, run_various_stock] >> combine_stock_df_task
    combine_stock_df_task >> save_df_to_csv_task >> upload_csv_to_s3_bucket_task >> done_task