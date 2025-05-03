import os
from datetime import datetime, timedelta

import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from us.assets.index.index_hanlder import IndexDataHandler
from us.utils.market_status import is_market_open
from us.utils.slack_alert import SlackAlert
from us.utils.upload_df_to_s3 import upload_dataframe_to_bucket


def default_args():
    return {
        "start_date": pendulum.datetime(2025, 1, 26, tz="Asia/Seoul"),
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": send_failure_slack_alert,
        # "on_success_callback": send_success_slack_alert,
    }


def create_stock_market_bucket_path(region, dir_name, partition_date, file_name):
    bucket_variable_key = "secret_s3_bucket_stock_market"
    bucket_name = Variable.get(bucket_variable_key)

    bucket_path_components = [bucket_name, region, dir_name, partition_date, file_name]
    bucket_path = f"s3://{os.sep.join(bucket_path_components)}"

    return bucket_path


def get_index_data(index_name, **kwargs):
    index_data_handler = IndexDataHandler()
    index_data = index_data_handler.fetch_index_components(index_name)

    # 데이터를 XCom에 저장
    ti = kwargs["ti"]
    ti.xcom_push(key=f"{index_name}_data", value=index_data)


def upload_index_data(index_name, **kwargs):
    ti = kwargs["ti"]
    xcom_pull_result = ti.xcom_pull(
        key=f"{index_name}_data", task_ids="get_index_tasks"
    )
    # LazyXComSelectSequence 유형이므로 데이터프레임 객체를 리스트에서 가져오기
    index_data = xcom_pull_result[0]

    partition_date = kwargs["data_interval_start"].to_date_string()
    index_data["partition_date"] = partition_date

    region = "us"
    dir_name = "index"
    file_type = "parquet"
    file_name = f"{index_name}_{partition_date}.{file_type}"

    bucket_path = create_stock_market_bucket_path(
        region, dir_name, partition_date, file_name
    )
    upload_dataframe_to_bucket(index_data, bucket_path)


def send_success_slack_alert(context):
    SlackAlert().create_success_alert(context)


def send_failure_slack_alert(context):
    SlackAlert().create_failure_alert(context)


def send_market_closed_slack_alert(context):
    SlackAlert().create_market_close_alert(context)


with DAG(
    dag_id="stock_market_us_index_v1_dag",
    description="미국 시장에서 주요 지수의 티커 정보를 위키피디아로부터 가져와서 S3 저장",
    schedule_interval="0 10 * * *",
    dagrun_timeout=timedelta(minutes=5),
    default_args=default_args(),
    catchup=True,
    on_success_callback=send_success_slack_alert,
    # catchup=False,
    tags=["us", "index"],
) as dag:
    index_list = ["S&P500", "NASDAQ100", "DOW30"]

    market_open_status = BranchPythonOperator(
        task_id="market_open_status",
        python_callable=is_market_open,
    )

    market_opend = EmptyOperator(task_id="market_opened")
    market_closed = PythonOperator(
        task_id="market_closed",
        python_callable=lambda **context: send_market_closed_slack_alert(context),
    )

    # Dynamic Mapping
    get_index_tasks = PythonOperator.partial(
        task_id="get_index_tasks",
        python_callable=get_index_data,
    ).expand(op_kwargs=[{"index_name": index} for index in index_list])

    upload_index_tasks = PythonOperator.partial(
        task_id="upload_index_tasks",
        python_callable=upload_index_data,
    ).expand(op_kwargs=[{"index_name": index} for index in index_list])

    # 마지막 Task (데이터 처리 후 종료)
    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    market_open_status >> market_opend
    market_open_status >> market_closed >> done_task
    market_opend >> get_index_tasks >> upload_index_tasks >> done_task
