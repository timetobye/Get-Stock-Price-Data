import pytz
import sys
sys.path.append('/opt/airflow/')

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from utils.market_open_status import GetKoreaMarketOpenStatus
from project.kr_market.stock_price import GetDailyStockData
from datetime import datetime


def convert_utc_to_kst(utc_time):
    kst_timezone = pytz.timezone('Asia/Seoul')
    kst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(kst_timezone)

    return kst_time


with DAG(
    dag_id="kr_market_stock_daily_price_download",
    start_date= datetime(2023, 11, 6, 9),
    schedule_interval='40 8 * * 1-5',  # 한국 기준 월 - 금
    # default_args=default_args,
    catchup=False
) as dag:
    get_daily_stock_data = GetDailyStockData()

    def get_market_open_status(**kwargs):
        check_market_open_status = GetKoreaMarketOpenStatus()

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        print(f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
              f' kwargs["data_interval_end"].strftime("%Y%m%d") : '
              f' {kwargs["data_interval_end"].strftime("%Y%m%d")}'
              f' kst_data_interval_end : {kst_data_interval_end}'
              f' target_date : {target_date}')

        open_yn_result = check_market_open_status.get_kr_market_status(target_date)
        kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)

    def check_open_status(**kwargs):
        open_yn_result = kwargs["ti"].xcom_pull(key="open_yn_result", task_ids="get_market_open_status")
        # open_yn_result = "N"

        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_open_task"

    def read_and_pass_stock_code_data(**kwargs):
        # TODO : 변수명 적절한 이름으로 리팩토링 하기
        stock_code_data = get_daily_stock_data.read_stock_code_data()
        kwargs["ti"].xcom_push(key="stock_code_data", value=stock_code_data)

        return stock_code_data

    def download_stock_data_csv(**kwargs):
        stock_info_dict = kwargs["ti"].xcom_pull(
            key="stock_code_data", task_ids='read_and_pass_stock_code'
        )

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")
        get_daily_stock_data.concat_stock_dataframe(stock_info_dict, target_date)

    def send_slack_message(**kwargs):
        dag_id = kwargs['dag'].dag_id
        # execution_date = kwargs['execution_date']

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        slack_message = f"DAG execution completed! " \
                        f"DAG ID : {dag_id} " \
                        f"kst data interval end : {kst_data_interval_end} " \
                        f"target_date : {target_date}"

        task_instance = kwargs['ti']
        task_instance.xcom_push(
            key="slack_message",
            value=slack_message
        )

    get_market_open_status = PythonOperator(
        task_id="get_market_open_status",
        python_callable=get_market_open_status,
        provide_context=True
    )

    check_open_status = BranchPythonOperator(
        task_id="check_open_status",
        python_callable=check_open_status,
        provide_context=True
    )

    read_and_pass_stock_code_task = PythonOperator(
        task_id="read_and_pass_stock_code",
        python_callable=read_and_pass_stock_code_data,
        provide_context=True
    )

    download_stock_data_csv_task = PythonOperator(
        task_id="download_stock_data_csv",
        python_callable=download_stock_data_csv,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_open_task = EmptyOperator(task_id="market_open_task")

    make_slack_message_task = PythonOperator(
        task_id="make_slack_message",
        python_callable=get_daily_stock_data.make_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token=get_daily_stock_data.get_slack_message_token(),
        channel=get_daily_stock_data.get_slack_channel_name(),
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    get_market_open_status >> check_open_status
    check_open_status >> market_closed_task >> done_task
    check_open_status >> market_open_task
    market_open_task >> read_and_pass_stock_code_task >> download_stock_data_csv_task
    download_stock_data_csv_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task
