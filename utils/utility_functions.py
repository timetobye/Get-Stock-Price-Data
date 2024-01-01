# Utility functions...
import os
import shutil


class UtilityFunctions:
    def __init__(self):
        pass

    @staticmethod
    def make_data_directory_path():
        from airflow.models import Variable
        dir_json = Variable.get(key="directory", deserialize_json=True)
        data_dir_base_name = dir_json["data"]  # "airflow/data"

        parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
        data_directory_path = os.path.join(parent_directory_path, data_dir_base_name)

        return data_directory_path

    @staticmethod
    def remove_files_in_directory(directory_path):
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)

            if os.path.isfile(item_path):
                # 파일일 경우 제거
                os.remove(item_path)
            elif os.path.isdir(item_path):
                # 디렉터리일 경우 shutil.rmtree()를 사용하여 재귀적으로 제거
                shutil.rmtree(item_path)

    @staticmethod
    def get_est_date_from_utc_time(pendulum_utc_datetime):
        # utc_time : pendulum.datetime - context["data_interval_end"] or context["data_interval_start"]
        est_time = pendulum_utc_datetime.in_timezone("America/New_York")
        est_date = est_time.to_date_string().replace('-', '')  # "%Y%m%d"

        return est_date

    @staticmethod
    def upload_file_to_s3_bucket(dir_path, bucket_name, file_extension_name=".csv"):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_connection_str = 's3_conn'
        s3_hook = S3Hook(s3_connection_str)

        os_walk_from_dir_path = os.walk(dir_path)
        for root, dirs, files in os_walk_from_dir_path:
            for file in files:
                if file.lower().endswith(file_extension_name):
                    local_file_path = os.path.join(root, file)

                    s3_key = os.path.relpath(local_file_path, dir_path)
                    s3_hook.load_file(
                        filename=local_file_path,
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True
                    )


