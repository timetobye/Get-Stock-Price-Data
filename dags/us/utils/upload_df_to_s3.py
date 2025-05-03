import awswrangler as wr
import pandas as pd
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def upload_dataframe_to_bucket(df, bucket_path):
    # 1️⃣ Airflow에서 AWS 세션 정보 가져오기
    aws_hook = AwsBaseHook(aws_conn_id="aws_default", client_type="s3")
    session = aws_hook.get_session()  # boto3 Session 생성

    # # 2️⃣ 저장할 데이터프레임 생성
    # df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

    # 2️⃣ AWS Wrangler를 사용하여 S3에 CSV 저장
    wr.s3.to_parquet(
        df=df,
        path=bucket_path,
        index=False,
        boto3_session=session,  # Airflow AWS 세션 적용
    )

    print("✅ 데이터가 S3에 성공적으로 업로드되었습니다!")
