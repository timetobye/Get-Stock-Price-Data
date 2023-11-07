import sys
sys.path.append('/opt/airflow/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from datetime import datetime
from project.kr_market.upload_file_to_s3 import UploadCsvToS3


with DAG(
    dag_id="kr_market_upload_stock_data_to_s3",
    start_date=datetime(2023, 11, 6, 9),
    schedule_interval='0 9 * * 1-5',  # 한국 기준 월 - 토
    catchup=False,
    tags=["CSV", "S3", "UPLOAD"]
) as dag:
    csv_to_s3 = UploadCsvToS3()

    upload_csv_to_s3_task = PythonOperator(
        task_id="upload_csv_to_s3",
        python_callable=csv_to_s3.upload_csv_to_s3,
        provide_context=True
    )

    done_upload_task = EmptyOperator(
        task_id="done_upload_task",
        trigger_rule="none_failed"
    )

    make_slack_message_task = PythonOperator(
        task_id="make_slack_message",
        python_callable=csv_to_s3.make_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token=csv_to_s3.get_slack_message_token(),
        channel=csv_to_s3.get_slack_channel_name(),
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    upload_csv_to_s3_task >> done_upload_task
    done_upload_task >> make_slack_message_task >> slack_notification_task

