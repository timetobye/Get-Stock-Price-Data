import os
from datetime import timedelta
from pprint import pprint

import awswrangler as wr
import pandas as pd
import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from us.assets.ticker.ticker_handler import (
    download_tickers_price,
    merge_ticker_batch_dataframes,
)
from us.utils.market_status import is_market_open
from us.utils.maximum_drawdown import get_stock_close_series_data
from us.utils.slack_alert import SlackAlert
from us.utils.sql_handler import get_query_from_file
from us.utils.upload_df_to_s3 import upload_dataframe_to_bucket
from us.utils.wragler_utils import read_data_with_athena


def default_args():
    return {
        "start_date": pendulum.datetime(2025, 4, 19, tz="Asia/Seoul"),
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
    # ticker_list = query_result["ticker"].tolist()[0:10]
    kwargs["ti"].xcom_push(key="ticker_list", value=ticker_list)


def get_ticker_batches_index(**kwargs):
    ti = kwargs["ti"]
    ticker_list = ti.xcom_pull(
        task_ids="get_ticker_list_from_aws_task", key="ticker_list"
    )

    batch_size = 50  # 원하는 배치 크기 설정
    total_batches = (len(ticker_list) + batch_size - 1) // batch_size  # 올림 계산
    return list(range(total_batches))  # 인덱스 리스트 반환


def create_dynamic_upload_tasks(idx, **kwargs):
    ti = kwargs["ti"]
    batch_json_data = ti.xcom_pull(
        task_ids=f"fetch_ticker_price_tasks", key=f"batch_{idx}"
    )
    if batch_json_data:
        batch_df = pd.read_json(batch_json_data[0])

    batch_ticker_list = batch_df["ticker"].unique().tolist()

    upload_mdd_df_to_s3(batch_ticker_list, batch_df)


def upload_mdd_df_to_s3(ticker_list, df, **kwargs):
    bucket_variable_key = "secret_s3_bucket_stock_market"
    bucket_name = Variable.get(bucket_variable_key)
    # 1️⃣ Airflow에서 AWS 세션 정보 가져오기
    aws_hook = AwsBaseHook(
        aws_conn_id="aws_default", client_type="s3", region_name="ap-northeast-2"
    )
    session = aws_hook.get_session()  # boto3 Session 생성

    region = "us"
    dir_name = "mdd"
    file_type = "parquet"

    df["duration"] = df["duration"].astype("float")
    preprocess_column = ["peak_date", "valley_date", "recovery_date"]
    # 각 컬럼을 datetime으로 변환 후 YYYY-MM-DD 형식 문자열로 덮어쓰기
    for col in preprocess_column:
        df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )

    for ticker in ticker_list:
        mdd_ticker = df[df["ticker"] == ticker].copy()
        if mdd_ticker.empty:
            continue

        # 밀리초 단위를 datetime으로 변환 (NaN도 안전하게 처리)

        file_name = f"{ticker}_mdd.{file_type}"
        bucket_path_components = [bucket_name, region, dir_name, ticker, file_name]
        bucket_path = f"s3://{os.sep.join(bucket_path_components)}"

        wr.s3.to_parquet(
            df=mdd_ticker, path=bucket_path, index=False, boto3_session=session
        )
        print(f"✅ {ticker} mdd 데이터가 S3에 성공적으로 업로드되었습니다!")


with DAG(
    dag_id="stock_market_mdd_v1_dag",
    description="미국 시장에서 MDD 확인",
    schedule_interval="0 1 * * 7",
    dagrun_timeout=timedelta(minutes=5),
    default_args=default_args(),
    catchup=False,
    # render_template_as_native_obj=True,
    on_success_callback=send_success_slack_alert,
    tags=["us", "index", "ticker"],
) as dag:
    get_ticker_list_from_aws_task = PythonOperator(
        task_id="get_ticker_list_from_aws_task",
        python_callable=get_ticker_list_from_aws,
    )

    #
    get_ticker_batches_task = PythonOperator(
        task_id="get_ticker_batches", python_callable=get_ticker_batches_index
    )

    # 병렬로 데이터 가져오기
    fetch_ticker_price_tasks = PythonOperator.partial(
        task_id="fetch_ticker_price_tasks",
        python_callable=get_stock_close_series_data,
    ).expand(op_kwargs=get_ticker_batches_task.output.map(lambda idx: {"idx": idx}))

    # 각 Ticker 별 업로드
    dynamic_upload_tasks = PythonOperator.partial(
        task_id="dynamic_upload_tasks",
        python_callable=create_dynamic_upload_tasks,
    ).expand(op_kwargs=get_ticker_batches_task.output.map(lambda idx: {"idx": idx}))

    # 마지막 Task (데이터 처리 후 종료)
    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    get_ticker_list_from_aws_task >> get_ticker_batches_task >> fetch_ticker_price_tasks
    fetch_ticker_price_tasks >> dynamic_upload_tasks >> done_task
