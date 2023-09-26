import boto3
import configparser
import os
import pytz
import pandas as pd
import urllib.request
import zipfile
import glob

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from datetime import datetime


class DownloadKoreaMarketCode:
    def __init__(self):
        self.download_directory_name = "airflow/data"

    def get_download_zip_file_path(self, file_name):
        parent_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
        download_zip_file_path = os.path.join(
            parent_directory,
            self.download_directory_name,
            file_name
        )

        return download_zip_file_path

    def download_kospi_stock_code_file(self):
        file_name = "kospi_code.zip"
        download_kospi_zip_file_path = self.get_download_zip_file_path(file_name)

        download_zip_file_url = f"https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
        urllib.request.urlretrieve(download_zip_file_url, download_kospi_zip_file_path)

    def unzip_kospi_stock_code_zip_file(self):
        file_name = "kospi_code.zip"
        zip_file_path = self.get_download_zip_file_path(file_name)
        zip_file_directory = zip_file_path.replace(file_name, "")

        kr_market_zip = zipfile.ZipFile(zip_file_path)
        kr_market_zip.extractall(zip_file_directory)
        kr_market_zip.close()
        os.remove(zip_file_path)

    def download_kosdaq_stock_code_file(self):
        file_name = "kosdaq_code.zip"
        download_kosdaq_zip_file_path = self.get_download_zip_file_path(file_name)

        download_zip_file_url = f"https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"
        urllib.request.urlretrieve(download_zip_file_url, download_kosdaq_zip_file_path)

    def unzip_kosdaq_stock_code_zip_file(self):
        file_name = "kosdaq_code.zip"
        zip_file_path = self.get_download_zip_file_path(file_name)
        zip_file_directory = zip_file_path.replace(file_name, "")

        kr_market_zip = zipfile.ZipFile(zip_file_path)
        kr_market_zip.extractall(zip_file_directory)
        kr_market_zip.close()
        os.remove(zip_file_path)

    def get_kospi_master_dataframe(self, **kwargs):
        mst_file_path = self.get_download_zip_file_path("kospi_code.mst")
        tmp_file1 = self.get_download_zip_file_path("kospi_code_part1.tmp")
        tmp_file2 = self.get_download_zip_file_path("kospi_code_part2.tmp")

        wf1 = open(tmp_file1, mode="w")
        wf2 = open(tmp_file2, mode="w")

        with open(mst_file_path, mode="r", encoding="cp949") as f:
            for row in f:
                rf1 = row[0:len(row) - 228]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
                rf2 = row[-228:]
                wf2.write(rf2)

        wf1.close()
        wf2.close()

        part1_columns = ['단축코드', '표준코드', '한글종목명']
        df1 = pd.read_csv(tmp_file1, header=None, names=part1_columns, encoding='utf-8')

        field_specs = [2, 1, 4, 4, 4,
                       1, 1, 1, 1, 1,
                       1, 1, 1, 1, 1,
                       1, 1, 1, 1, 1,
                       1, 1, 1, 1, 1,
                       1, 1, 1, 1, 1,
                       1, 9, 5, 5, 1,
                       1, 1, 2, 1, 1,
                       1, 2, 2, 2, 3,
                       1, 3, 12, 12, 8,
                       15, 21, 2, 7, 1,
                       1, 1, 1, 1, 9,
                       9, 9, 5, 9, 8,
                       9, 3, 1, 1, 1
                       ]

        part2_columns = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
                         '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
                         'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
                         'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
                         'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
                         'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
                         'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
                         '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
                         '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
                         '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
                         '상장주수', '자본금', '결산월', '공모가', '우선주',
                         '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
                         '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
                         '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
                         ]

        df2 = pd.read_fwf(tmp_file2, widths=field_specs, names=part2_columns, encoding='cp949')

        df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)
        # ref : https://wikidocs.net/178118

        reits_df = df[df['그룹코드']=='RT']
        infra_df = df[df['그룹코드']=='IF']
        stock_type_1_df = df[(df['그룹코드'] == 'ST') & (df['시가총액'] >= 4000)]  # 시가총액 4000억 이상
        stock_type_2_df = df[(df['그룹코드'] == 'ST') & (df['KRX300'] >= 'Y')]  # KRX300 종목

        bank_keywords = ['은행', '금융', '신한', '카카오뱅크']
        pattern = '|'.join(bank_keywords)
        bank_df = df[(df['그룹코드'] == 'ST') & (df['한글종목명'].str.contains(pattern))]

        dfs = [reits_df, infra_df, stock_type_1_df, stock_type_2_df, bank_df]
        df = pd.concat(dfs, ignore_index=True)
        df.drop_duplicates(inplace=True)

        # group_code_selection_list = ['ST', 'EF', 'RT', 'IF']
        # df = df[df['그룹코드'].isin(group_code_selection_list)]

        # clean temporary file and dataframe
        del (df1)
        del (df2)
        os.remove(mst_file_path)
        os.remove(tmp_file1)
        os.remove(tmp_file2)

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        file_name = f'kospi_code_{target_date}.csv'
        csv_file_path = self.get_download_zip_file_path(file_name)

        df.to_csv(csv_file_path, index=False)  # 지정된 위치에 엑셀파일로 저장
        print("Done : Kospi code")

    def get_kosdaq_master_dataframe(self, **kwargs):
        mst_file_path = self.get_download_zip_file_path("kosdaq_code.mst")
        tmp_file1 = self.get_download_zip_file_path("kosdaq_code_part1.tmp")
        tmp_file2 = self.get_download_zip_file_path("kosdaq_code_part2.tmp")

        wf1 = open(tmp_file1, mode="w")
        wf2 = open(tmp_file2, mode="w")

        with open(mst_file_path, mode="r", encoding="cp949") as f:
            for row in f:
                rf1 = row[0:len(row) - 222]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
                rf2 = row[-222:]
                wf2.write(rf2)

        wf1.close()
        wf2.close()

        part1_columns = ['단축코드', '표준코드', '한글종목명']
        df1 = pd.read_csv(tmp_file1, header=None, names=part1_columns, encoding='utf-8')

        field_specs = [2, 1,
                       4, 4, 4, 1, 1,
                       1, 1, 1, 1, 1,
                       1, 1, 1, 1, 1,
                       1, 1, 1, 1, 1,
                       1, 1, 1, 1, 9,
                       5, 5, 1, 1, 1,
                       2, 1, 1, 1, 2,
                       2, 2, 3, 1, 3,
                       12, 12, 8, 15, 21,
                       2, 7, 1, 1, 1,
                       1, 9, 9, 9, 5,
                       9, 8, 9, 3, 1,
                       1, 1
                       ]

        part2_columns = ['그룹코드', '시가총액규모_구분코드_유가',
                         '지수업종_대분류_코드', '지수업종_중분류_코드', '지수업종소분류코드', '벤처기업여부',
                         '저유동성종목여부', 'KRX종목여부', 'ETP상품구분코드', 'KRX100종목여부',
                         'KRX자동차여부', 'KRX반도체여부', 'KRX바이오여부', 'KRX은행여부', '기업인수목적회사여부',
                         'KRX에너지화학여부', 'KRX철강여부', '단기과열종목구분코드', 'KRX미디어통신여부',
                         'KRX건설여부', '투자주의환기종목여부', 'KRX증권구분', 'KRX선박구분',
                         'KRX섹터지수 보험여부', 'KRX섹터지수 운송여부', 'KOSDAQ150지수여부', '주식기준가',
                         '정규시장매매수량단위', '시간외시장매매수량단위', '거래정지여부', '정리매매여부',
                         '관리종목여부', '시장경고구분코드', '시장경고위험예고여부', '불성실공시여부',
                         '우회상장여부', '락구분코드', '액면가변경구분코드', '증자구분코드', '증거금비율',
                         '신용주문가능여부', '신용기간', '전일거래량', '주식액면가', '주식상장일자', '상장_주수_천',
                         '자본금', '결산월', '공모가격', '우선주구분코드', '공매도과열종목여부', '이상급등종목여부',
                         'KRX300', '매출액', '영업이익', '경상이익', '당기순이익', 'ROE',
                         '기준년월', '시가총액', '그룹사코드', '회사신용한도초과여부', '담보대출가능여부', '대주가능여부'
                         ]

        df2 = pd.read_fwf(tmp_file2, widths=field_specs, names=part2_columns)
        df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

        stock_type_1_df = df[(df['그룹코드'] == 'ST') & (df['시가총액'] >= 4000)]  # 시가총액 1000억 이상
        stock_type_2_df = df[(df['그룹코드'] == 'ST') & (df['KRX300'] >= 'Y')]  # KRX300 종목
        stock_type_3_df = df[(df['그룹코드'] == 'ST') & (df['KOSDAQ150지수여부'] >= 'Y')]  # KOSDAQ150지수여부 종목

        dfs = [stock_type_1_df, stock_type_2_df, stock_type_3_df]
        df = pd.concat(dfs, ignore_index=True)
        df.drop_duplicates(inplace=True)

        # clean temporary file and dataframe
        del (df1)
        del (df2)
        os.remove(mst_file_path)
        os.remove(tmp_file1)
        os.remove(tmp_file2)

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        file_name = f'kosdaq_code_{target_date}.csv'
        csv_file_path = self.get_download_zip_file_path(file_name)

        df.to_csv(csv_file_path, index=False)  # 현재 위치에 엑셀파일로 저장
        print("Done : Kosdaq code")


class UploadKoreaMarketCodeFileToS3:
    def __init__(self):
        self.config_ini_file_name = "config.ini"
        self.configuration_directory_name = "airflow/config"
        self.config_file_path = self.get_config_file_path()
        self.config_object = self.get_config_object()

        self.csv_directory_name = "airflow/data"
        self.csv_file_paths = self.get_csv_file_path()

        self.bucket_name = 'your bucket name'  # 한국 거래소 파일 업로드용 버킷 입니다.

    def get_config_file_path(self):
        configuration_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
        config_file_path = os.path.join(
            configuration_directory,
            self.configuration_directory_name,
            self.config_ini_file_name
        )
        print(f"self.config_file_path : {config_file_path}")

        return config_file_path

    def get_config_object(self):
        config_object = configparser.ConfigParser()
        config_object.read(self.config_file_path)

        return config_object

    def get_csv_file_path(self):
        csv_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
        csv_file_path = os.path.join(
            csv_directory,
            self.csv_directory_name
        )
        print(f"self.csv_file_path : {csv_file_path}")

        # csv_file_names = os.listdir(self.csv_file_path)
        csv_file_paths = glob.glob(f"{csv_file_path}{os.sep}*.csv")

        return csv_file_paths

    def delete_csv_file(self, path):
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

        print(f"self.csv_file_paths : {self.csv_file_paths} ")

        for file_path in self.csv_file_paths:
            file_name = os.path.basename(file_path)
            date_string = file_name.split(".")[0].split("_")[-1]  # date parsing

            print(f"date_string : {date_string} **********")
            year, month, day = date_string[0:4], date_string[4:6], date_string[6:]
            print(f"**** year : month, day = {year} : {month} : {day} *********")

            s3_path = f"{year}{os.sep}{month}{os.sep}{day}{os.sep}"
            s3_client.upload_file(file_path, self.bucket_name, f"{s3_path}{file_name}")
            print(f"Complete upload : {s3_path}{file_name}")

    def get_slack_message_token(self):
        slack_message_token = self.config_object.get('SLACK_CONFIG', 'channel_access_token')

        return slack_message_token

    def get_slack_channel_name(self):
        channel_name = self.config_object.get('SLACK_CONFIG', 'channel_name')
        slack_channel_name = f"#{channel_name}"

        return slack_channel_name

    def make_slack_message(self, **kwargs):
        dag_id = kwargs['dag'].dag_id
        # execution_date = kwargs['execution_date']

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

download_code_file = DownloadKoreaMarketCode()
upload_csv_to_s3 = UploadKoreaMarketCodeFileToS3()


def convert_utc_to_kst(utc_time):
    try:
        kst_timezone = pytz.timezone('Asia/Seoul')
        kst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(kst_timezone)

        return kst_time
    except Exception as e:
        print(f"Error during timezone conversion : {e}")

        raise ValueError("Error during timezone conversion") from e

# TODO : 여기 부분 dag_id 이름 바꾸기 - 전체적으로 수정 해야 함
with DAG(
    dag_id="kr_market_stock_code_download_csv_and_upload_to_s3",
    start_date=datetime(2023, 9, 25, 9),
    schedule_interval='30 8 * * 1-5',  # 한국 기준 월 - 금
    catchup=False,
    tags=["CSV", "S3", "UPLOAD"]
) as dag:
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






