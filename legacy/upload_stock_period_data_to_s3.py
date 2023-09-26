import boto3
import os
import pandas as pd

BUCKET_NAME = 'INPUT BUCKET NAME'


def check_target_directory():
    # stock_csv_files 디렉터리를 체크하고 없을 경우 생성합니다.
    current_directory = os.getcwd()
    stock_csv_files_directory_path = f"{current_directory}{os.sep}stock_csv_files"

    isdir = os.path.isdir(stock_csv_files_directory_path)
    if not isdir:
        os.mkdir(stock_csv_files_directory_path)


def get_csv_file_paths():
    """
    csv file path 를 읽은 후 CSV 파일 변환 및 S3로 보내기 위한 준비 작업을 진행합니다.
    :return: csv_file_paths
    """
    current_directory = os.getcwd()
    stock_csv_files_directory_path = f"{current_directory}{os.sep}stock_csv_files"
    csv_file_names = os.listdir(stock_csv_files_directory_path)
    csv_file_paths = []

    for file_name in csv_file_names:
        csv_file_path = f"{stock_csv_files_directory_path}{os.sep}{file_name}"
        csv_file_paths.append(csv_file_path)

    return csv_file_paths


def delete_csv_file(path):
    # S3 로 CSV 를 전송을 한 후 해당 경로에 해당하는 파일을 제거합니다.
    os.remove(path)


def main():
    file_paths = get_csv_file_paths()
    s3_client = boto3.client('s3')

    for file_path in file_paths:
        df = pd.read_csv(file_path)
        target_date_list = df['stck_bsop_date'].unique().tolist()
        stock_code = file_path.split('/')[-1].split('_')[0]

        for target_date in target_date_list:
            year, month, day = target_date.split('-')
            target_df = df[df['stck_bsop_date'] == target_date]
            date_str_format = target_date.replace('-', '')

            # 해당 하는 일자에 맞게 CSV 로 바꿔서 저장
            target_csv_file_name = f"{stock_code}_{date_str_format}.csv"
            target_date_file_path = f"{os.getcwd()}{os.sep}stock_csv_files{os.sep}{target_csv_file_name}"
            target_df.to_csv(target_date_file_path, index=False)

            # 년 / 월 / 일 별로 CSV 를 S3 디렉토리에 보내는 부분
            object_name = os.path.basename(target_csv_file_name)
            object_path = f"{year}{os.sep}{month}{os.sep}{day}{os.sep}"

            with open(target_date_file_path, "rb") as f:
                s3_client.upload_fileobj(f, BUCKET_NAME, f"{object_path}{object_name}")
            delete_csv_file(target_date_file_path)


if __name__ == "__main__":
    main()