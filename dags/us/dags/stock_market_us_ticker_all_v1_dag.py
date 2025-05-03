import os
from datetime import datetime, timedelta

import awswrangler as wr
import pandas as pd
import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from us.utils.market_status import is_market_open
from us.utils.slack_alert import SlackAlert
from us.utils.sql_handler import get_query_from_file
from us.utils.upload_df_to_s3 import upload_dataframe_to_bucket
from us.utils.wragler_utils import read_data_with_athena


def default_args():
    return {
        "start_date": pendulum.datetime(2025, 1, 26, tz="Asia/Seoul"),
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": send_failure_slack_alert,
        # "on_success_callback": send_success_slack_alert,
    }


def send_success_slack_alert(context):
    SlackAlert().create_success_alert(context)


def send_failure_slack_alert(context):
    SlackAlert().create_failure_alert(context)


def send_market_closed_slack_alert(context):
    SlackAlert().create_market_close_alert(context)


def get_ticker_from_aws() -> pd.DataFrame:
    us_ticker_sql_file_name = "stock_market_us_ticker_list.sql"
    us_ticker_sql_query_string = get_query_from_file(us_ticker_sql_file_name)
    query_result = read_data_with_athena(us_ticker_sql_query_string)

    return query_result


def get_ticker_from_gsheet() -> pd.DataFrame:
    gsheet_hook = GSheetsHook(gcp_conn_id="gcp_default")

    gsheet_key = Variable.get("secret_google_sheet_key")
    gsheet_range = "Ticker_list!A2:A"

    gsheet_values = gsheet_hook.get_values(
        spreadsheet_id=gsheet_key, range_=gsheet_range
    )
    gsheet_df = pd.DataFrame(gsheet_values, columns=["ticker"])

    return gsheet_df


def concat_ticker_data(**kwargs) -> pd.DataFrame:
    ti = kwargs["ti"]
    aws_result = ti.xcom_pull(task_ids="get_ticker_from_aws_task")
    gsheet_result = ti.xcom_pull(task_ids="get_ticker_from_gsheet_task")

    concat_df = pd.concat([aws_result, gsheet_result], ignore_index=True)
    concat_df = concat_df.drop_duplicates()

    return concat_df


def create_stock_market_ticker_bucket_path(region, dir_name, file_name):
    bucket_variable_key = "secret_s3_bucket_stock_market"
    bucket_name = Variable.get(bucket_variable_key)

    bucket_path_components = [bucket_name, region, dir_name, file_name]
    bucket_path = f"s3://{os.sep.join(bucket_path_components)}"

    return bucket_path


# TODO : ticker 에 etf 인지 equity 인지 정보 붙이는 코드 개발
def upload_ticker_data(**kwargs):
    ti = kwargs["ti"]
    ticker_data = ti.xcom_pull(task_ids="concat_ticker_task")

    region = "us"
    dir_name = "ticker_list"
    file_type = "parquet"
    file_name = f"stock_market_us_ticker.{file_type}"

    bucket_path = create_stock_market_ticker_bucket_path(region, dir_name, file_name)
    upload_dataframe_to_bucket(ticker_data, bucket_path)


with DAG(
    dag_id="stock_market_ticker_all_v1_dag",
    description="미국 시장에서 티커 정보를 저장한 곳에서 가져와서 매일 정리합니다. 센서를 사용합니다.",
    schedule_interval="5 10 * * *",
    dagrun_timeout=timedelta(minutes=5),
    default_args=default_args(),
    catchup=False,
    on_success_callback=send_success_slack_alert,
    tags=["us", "index", "ticker"],
) as dag:
    market_open_status = BranchPythonOperator(
        task_id="market_open_status",
        python_callable=is_market_open,
    )

    market_opend = EmptyOperator(task_id="market_opened")
    market_closed = PythonOperator(
        task_id="market_closed",
        python_callable=lambda **context: send_market_closed_slack_alert(context),
    )

    wait_for_target_dag = ExternalTaskSensor(
        task_id="wait_for_target_dag",
        external_dag_id="stock_market_us_index_v1_dag",
        external_task_id="done_task",  # 전체 DAG를 기다린다면 None
        allowed_states=[State.SUCCESS],  # 완료 상태(success)일 때 실행
        failed_states=[State.FAILED],  # 실패 감지 시 에러 발생
        mode="poke",  # poke 모드 (주기적으로 감지)
        poke_interval=10,  # 10초마다 감지
        execution_delta=timedelta(minutes=5),
        timeout=120,
        check_existence=True,
    )

    get_ticker_from_aws_task = PythonOperator(
        task_id="get_ticker_from_aws_task", python_callable=get_ticker_from_aws
    )

    get_ticker_from_gsheet_task = PythonOperator(
        task_id="get_ticker_from_gsheet_task", python_callable=get_ticker_from_gsheet
    )

    concat_ticker_task = PythonOperator(
        task_id="concat_ticker_task",
        python_callable=concat_ticker_data,
        trigger_rule="all_success",
    )

    upload_ticker_task = PythonOperator(
        task_id="upload_index_tasks",
        python_callable=upload_ticker_data,
    )

    # 마지막 Task (데이터 처리 후 종료)
    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    market_open_status >> market_opend >> wait_for_target_dag
    market_open_status >> market_closed >> done_task

    (
        wait_for_target_dag
        >> [get_ticker_from_aws_task, get_ticker_from_gsheet_task]
        >> concat_ticker_task
    )
    concat_ticker_task >> upload_ticker_task >> done_task
