import os
from datetime import timedelta
from pprint import pprint

import pandas as pd
import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from us.assets.ticker.ticker_handler import (
    download_tickers_price,
    merge_ticker_batch_dataframes,
)
from us.utils.market_status import is_market_open
from us.utils.slack_alert import SlackAlert
from us.utils.sql_handler import get_query_from_file
from us.utils.upload_df_to_s3 import upload_dataframe_to_bucket
from us.utils.wragler_utils import read_data_with_athena


def default_args():
    return {
        "start_date": pendulum.datetime(2025, 3, 5, tz="Asia/Seoul"),
        # "retries": 1,
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


def get_ticker_list_from_aws(**kwargs) -> pd.DataFrame:
    us_ticker_sql_file_name = "stock_market_us_ticker_list.sql"
    us_ticker_sql_query_string = get_query_from_file(us_ticker_sql_file_name)
    query_result = read_data_with_athena(us_ticker_sql_query_string)

    ticker_list = query_result["ticker"].tolist()
    kwargs["ti"].xcom_push(key="ticker_list", value=ticker_list)


def get_ticker_batches_index(**kwargs):
    ti = kwargs["ti"]
    ticker_list = ti.xcom_pull(
        task_ids="get_ticker_list_from_aws_task", key="ticker_list"
    )

    batch_size = 50  # 원하는 배치 크기 설정
    total_batches = (len(ticker_list) + batch_size - 1) // batch_size  # 올림 계산
    return list(range(total_batches))  # 인덱스 리스트 반환


def create_stock_market_daily_price_path(region, dir_name, partition_date, file_name):
    bucket_variable_key = "secret_s3_bucket_stock_market"
    bucket_name = Variable.get(bucket_variable_key)

    bucket_path_components = [bucket_name, region, dir_name, partition_date, file_name]
    bucket_path = f"s3://{os.sep.join(bucket_path_components)}"

    return bucket_path


def upload_daily_price_data(**kwargs):
    ti = kwargs["ti"]
    ticker_data = ti.xcom_pull(key="merged_ticker_price_data")
    ticker_data["dividends"] = ticker_data["dividends"].astype("float")

    print(f'kwargs["data_interval_start"] : {kwargs["data_interval_start"]}')
    print(
        f'kwargs["data_interval_start"].in_timezone("Asia/Seoul") : {kwargs["data_interval_start"].in_timezone("Asia/Seoul")}'
    )

    partition_date = (
        kwargs["data_interval_start"].in_timezone("Asia/Seoul").to_date_string()
    )
    target_date = partition_date.replace("-", "")

    region = "us"
    dir_name = "daily_price"
    file_type = "parquet"
    file_name = f"us_daily_price_{target_date}_{target_date}.{file_type}"

    bucket_path = create_stock_market_daily_price_path(
        region, dir_name, partition_date, file_name
    )
    upload_dataframe_to_bucket(ticker_data, bucket_path)


def create_stock_market_ticker_price_path(region, dir_name, ticker, file_name):
    bucket_variable_key = "secret_s3_bucket_stock_market"
    bucket_name = Variable.get(bucket_variable_key)

    bucket_path_components = [bucket_name, region, dir_name, ticker, file_name]
    ticker_price_path = f"s3://{os.sep.join(bucket_path_components)}"

    return ticker_price_path


def create_dynamic_upload_tasks(idx, **kwargs):
    ti = kwargs["ti"]
    batch_json_data = ti.xcom_pull(
        task_ids=f"fetch_ticker_price_tasks", key=f"batch_{idx}"
    )
    if batch_json_data:
        batch_df = pd.read_json(batch_json_data[0])

    partition_date = (
        kwargs["data_interval_start"].in_timezone("Asia/Seoul").to_date_string()
    )
    target_date = partition_date.replace("-", "")
    batch_ticker_list = batch_df["ticker"].unique().tolist()

    upload_ticker_price_data(batch_ticker_list, batch_df, target_date)


# 각 티커에 대해 데이터를 Parquet 형식으로 S3에 업로드하는 함수
def upload_ticker_price_data(tickers_batch, df, target_date, **kwargs):
    region = "us"
    dir_name = "ticker_price"
    file_type = "parquet"

    # drop_column = ["ticker"]
    df["date"] = df["date"].astype(str)
    df["dividends"] = df["dividends"].astype(float)

    for ticker in tickers_batch:
        # 해당 티커의 데이터 필터링
        df_ticker = df[df["ticker"] == ticker].copy()

        if df_ticker.empty:
            continue

        file_name = f"{ticker}_{target_date}_{target_date}.{file_type}"
        s3_path = create_stock_market_ticker_price_path(
            region, dir_name, ticker, file_name
        )

        upload_dataframe_to_bucket(df_ticker, s3_path)


with DAG(
    dag_id="stock_market_ticker_price_v1_dag",
    description="미국 시장에서 티커 가격을 저장합니다. 센서를 사용합니다.",
    schedule_interval="30 10 * * *",
    dagrun_timeout=timedelta(minutes=5),
    default_args=default_args(),
    catchup=False,
    # render_template_as_native_obj=True,
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

    get_ticker_list_from_aws_task = PythonOperator(
        task_id="get_ticker_list_from_aws_task",
        python_callable=get_ticker_list_from_aws,
    )

    get_ticker_batches_task = PythonOperator(
        task_id="get_ticker_batches", python_callable=get_ticker_batches_index
    )

    # 병렬로 데이터 가져오기
    fetch_ticker_price_tasks = PythonOperator.partial(
        task_id="fetch_ticker_price_tasks",
        python_callable=download_tickers_price,
    ).expand(op_kwargs=get_ticker_batches_task.output.map(lambda idx: {"idx": idx}))

    # 데이터 병합 태스크 추가
    merge_ticker_data_task = PythonOperator(
        task_id="merge_ticker_data_task", python_callable=merge_ticker_batch_dataframes
    )

    # daily price 업로드 - 모든 종목에 대한 일일 가격
    upload_daily_price_task = PythonOperator(
        task_id="upload_daily_price_task",
        python_callable=upload_daily_price_data,
    )

    # 각 Ticker 별 업로드
    dynamic_upload_tasks = PythonOperator.partial(
        task_id="dynamic_upload_tasks",
        python_callable=create_dynamic_upload_tasks,
    ).expand(op_kwargs=get_ticker_batches_task.output.map(lambda idx: {"idx": idx}))

    # 마지막 Task (데이터 처리 후 종료)
    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    market_open_status >> market_opend >> get_ticker_list_from_aws_task
    market_open_status >> market_closed >> done_task
    get_ticker_list_from_aws_task >> get_ticker_batches_task >> fetch_ticker_price_tasks
    (
        fetch_ticker_price_tasks
        >> merge_ticker_data_task
        >> upload_daily_price_task
        >> done_task
    )
    fetch_ticker_price_tasks >> dynamic_upload_tasks >> done_task
