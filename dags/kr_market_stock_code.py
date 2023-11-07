import sys
sys.path.append('/opt/airflow/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from datetime import datetime
from project.kr_market.stock_code_manage import DownloadKoreaMarketCode
from project.kr_market.stock_code_manage import UploadKoreaMarketCodeFileToS3

with DAG(
    dag_id="kr_market_stock_code_download_csv_and_upload_to_s3",
    start_date=datetime(2023, 11, 6, 9),
    schedule_interval='30 8 * * 1-5',  # 한국 기준 월 - 금
    catchup=False,
    tags=["CSV", "S3", "UPLOAD"]
) as dag:
    download_code_file = DownloadKoreaMarketCode()
    upload_csv_to_s3 = UploadKoreaMarketCodeFileToS3()

    download_kospi_code_file = PythonOperator(
        task_id="download_kospi_code_file",
        python_callable=download_code_file.download_kospi_stock_code_file,
        provide_context=True
    )

    unzip_kospi_code_zip_file = PythonOperator(
        task_id="unzip_kospi_code_zip_file",
        python_callable=download_code_file.unzip_kospi_stock_code_zip_file,
        provide_context=True
    )

    make_kospi_df = PythonOperator(
        task_id="make_kospi_df",
        python_callable=download_code_file.get_kospi_master_dataframe,
        provide_context=True
    )
    download_kosdaq_code_file = PythonOperator(
        task_id="download_kosdaq_code_file",
        python_callable=download_code_file.download_kosdaq_stock_code_file,
        provide_context=True
    )

    unzip_kosdaq_code_zip_file = PythonOperator(
        task_id="unzip_kosdaq_code_zip_file",
        python_callable=download_code_file.unzip_kosdaq_stock_code_zip_file,
        provide_context=True
    )

    make_kosdaq_df = PythonOperator(
        task_id="make_kosdaq_df",
        python_callable=download_code_file.get_kosdaq_master_dataframe,
        provide_context=True
    )

    download_done = EmptyOperator(
        task_id="download_KR_Market_code",
        trigger_rule="none_failed"
    )

    upload_csv_to_s3_task = PythonOperator(
        task_id="upload_csv_to_s3",
        python_callable=upload_csv_to_s3.upload_csv_to_s3,
        provide_context=True
    )

    done_task = EmptyOperator(
        task_id="done_task",
        trigger_rule="none_failed"
    )

    make_slack_message_task = PythonOperator(
        task_id="make_slack_message",
        python_callable=upload_csv_to_s3.make_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token=upload_csv_to_s3.get_slack_message_token(),
        channel=upload_csv_to_s3.get_slack_channel_name(),
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    download_kospi_code_file >> unzip_kospi_code_zip_file >> make_kospi_df
    download_kosdaq_code_file >> unzip_kosdaq_code_zip_file >> make_kosdaq_df

    make_kospi_df >> download_done
    make_kosdaq_df >> download_done

    download_done >> upload_csv_to_s3_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task
