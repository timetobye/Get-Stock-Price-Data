import boto3
import os
import pandas as pd
import pytz
import glob
import sys
sys.path.append('/opt/airflow/')

from utils.configuration_use import ConfigurationUse


def convert_utc_to_kst(utc_time):
    try:
        kst_timezone = pytz.timezone('Asia/Seoul')
        kst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(kst_timezone)
        return kst_time

    except Exception as e:
        print(f"Error during timezone conversion : {e}")

        raise ValueError("Error during timezone conversion") from e


class UploadCsvToS3(ConfigurationUse):
    def __init__(self):
        super().__init__()
        self.csv_directory_name = "airflow/data"
        self.csv_file_directory_path = self.get_csv_file_directory_path()
        self.csv_file_path_list = glob.glob(f"{self.csv_file_directory_path}{os.sep}*.csv")
        self.kr_bucket_name = 'your-bucket-name'  # kr market S3 bucket

    def get_csv_file_directory_path(self):
        """
        csv file path 를 읽은 후 CSV 파일 변환 및 S3로 보내기 위한 준비 작업을 진행합니다.
        :return: csv_file_paths
        """
        csv_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
        csv_file_directory_path = os.path.join(
            csv_directory,
            self.csv_directory_name
        )
        print(f"self.csv_file_path : {csv_file_directory_path}")

        return csv_file_directory_path

    def remove_csv_file(self, path):
        # S3 로 CSV 를 전송을 한 후 해당 경로에 해당하는 파일을 제거합니다.
        os.remove(path)

    def upload_csv_to_s3(self):
        aws_access_key = self.config_object.get('AWS_CONFIG', 'aws_access_key')
        aws_secret_access_key = self.config_object.get('AWS_CONFIG', 'aws_secret_access_key')

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key
        )

        for file_path in self.csv_file_path_list:
            file_path_token = file_path.split('/')[-1]
            market_region = file_path_token.split('_')[0]
            stock_code = file_path_token.split('_')[1]

            print(f"market_region : {market_region}")

            if market_region == "kr":
                bucket_name = self.kr_bucket_name
            else:
                continue  # TODO : Error 처리에 대한 기준 필요

            df = pd.read_csv(file_path)
            target_date_list = df['stck_bsop_date'].unique().tolist()

            # TODO : kospi, kosdaq 벌크로 들어올 떄, 개별 코드로 들어올 때 구분해서 하는 코드 추가해야함
            for target_date in target_date_list:
                year, month, day = target_date.split('-')
                target_df = df[df['stck_bsop_date'] == target_date]  # unique 값이 되어야 함
                date_str_format = target_date.replace('-', '')

                # 해당 하는 일자에 맞게 CSV 로 바꿔서 저장
                target_csv_file_name = f"{stock_code}_{date_str_format}.csv"
                target_date_file_path = f"{self.csv_file_directory_path}{os.sep}{target_csv_file_name}"
                target_df.to_csv(target_date_file_path, index=False)

                # 년 / 월 / 일 별로 CSV 를 S3 디렉토리에 보내는 부분
                object_name = os.path.basename(target_csv_file_name)  # 파일 이름만 출력
                print(f"object_name : {object_name}")

                # AWS Glue + Automatic partition
                object_path = f"{date_str_format}{os.sep}"
                s3_client.upload_file(target_date_file_path, bucket_name, f"{object_path}{object_name}")
                print(f"Complete upload : {object_path}{object_name}")

                self.remove_csv_file(target_date_file_path)
            self.remove_csv_file(file_path)

    def get_slack_message_token(self):
        slack_message_token = self.config_object.get('SLACK_CONFIG', 'channel_access_token')

        return slack_message_token

    def get_slack_channel_name(self):
        channel_name = self.config_object.get('SLACK_CONFIG', 'channel_name')
        slack_channel_name = f"#{channel_name}"

        return slack_channel_name

    def make_slack_message(self, **kwargs):
        dag_id = kwargs['dag'].dag_id
        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        slack_message = f"""
        **결과 요약**
        - DAG 작업: {dag_id}
        - 작업 날짜 : {target_date}
        - kst_data_interval_end : {kst_data_interval_end}
        """

        task_instance = kwargs['ti']
        task_instance.xcom_push(
            key="slack_message",
            value=slack_message
        )





