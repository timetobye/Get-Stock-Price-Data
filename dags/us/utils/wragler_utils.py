import awswrangler as wr
import pandas as pd
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def read_data_with_athena(query):
    # 1️⃣ Airflow에서 AWS 세션 정보 가져오기
    aws_hook = AwsBaseHook(
        aws_conn_id="aws_default", client_type="s3", region_name="ap-northeast-2"
    )
    session = aws_hook.get_session()  # boto3 Session 생성

    query_result = wr.athena.read_sql_query(
        query,
        database="stock_market",
        boto3_session=session,
    )

    return query_result
