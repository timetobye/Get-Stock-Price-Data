import boto3
import configparser
import os
import pandas as pd
import urllib.request
import requests
import pytz
import glob
import time
import zipfile

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from config_generate import GetUSAMarketOpenStatus
from datetime import datetime, timedelta


def convert_utc_to_pdt(utc_time):
    try:
        pdt_timezone = pytz.timezone('America/Los_Angeles')
        pdt_time = utc_time.replace(tzinfo=pytz.utc).astimezone(pdt_timezone)

        return pdt_time
    except Exception as e:
        print(f"Error during timezone conversion : {e}")

        raise ValueError("Error during timezone conversion") from e


class DownloadUSAMarketData(GetUSAMarketOpenStatus):
    """
    1. Index(S&P500, DOW30, NASDAQ100)에 속한 종목 정보를 우선 가져와야 합니다.
    2. 그 뒤에 한투에서 제공하는 해외주식 마스터 파일을 다운로드 해야 합니다.
    3. Index 데이터에서 종목 티커를 이용하여 마스터 파일에서 종목의 거래소 정보를 가져와야 합니다.
    4. 최종적으로 Index 데이터 + 거래소 정보를 결합한 CSV 파일을 하나 만듭니다.
    - 기타 사항 : 한투에서 제공해주는 마스터 파일의 데이터가 부정확 하거나 누락된 부분이 다소 있습니다.
    """
    def __init__(self):
        self.download_directory_name = "airflow/data"
        self.download_directory_path = self.get_download_directory_path()

    def get_download_directory_path(self):
        parent_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
        download_directory_path = os.path.join(
            parent_directory,
            self.download_directory_name
        )

        return download_directory_path

    def download_stock_data_for_index(self, **kwargs):
        """
        wikipedia 데이터는 데이터 업데이트가 빠르고, 기록 추적에 편리하기 때문에 사용합니다.
        한국투자증권에서 제공하는 API 에서 각 종목이 특정 지수에 속하는지에 대한 자료가 정확하지 않아 아래 코드를 이용하여 데이터 추적
        """
        data_interval_end = kwargs["data_interval_end"]
        pdt_data_interval_end = convert_utc_to_pdt(data_interval_end)
        target_date = pdt_data_interval_end.strftime("%Y%m%d")

        index_wiki_link_dict = {
            'S&P500': "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks",
            'NASDAQ100': "https://en.wikipedia.org/wiki/Nasdaq-100#Components",
            'DOW30': "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average#Components"
        }

        s_and_p_500_df = pd.read_html((index_wiki_link_dict.get('S&P500', None)), header=0)[0]
        nasdaq_100_df = pd.read_html((index_wiki_link_dict.get('NASDAQ100', None)), header=0)[4]
        dow_30_df = pd.read_html((index_wiki_link_dict.get('DOW30', None)), header=0)[1]

        s_and_p_500_column_selection = ['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']
        nasdaq_100_column_selection = ['Ticker', 'Company', 'GICS Sector', 'GICS Sub-Industry']
        dow_30_column_selection = ['Symbol', 'Company']

        s_and_p_500_df = s_and_p_500_df[s_and_p_500_column_selection]
        nasdaq_100_df = nasdaq_100_df[nasdaq_100_column_selection]
        dow_30_df = dow_30_df[dow_30_column_selection]

        s_and_p_500_column_rename = {'Security': 'Company'}
        nasdaq_100_column_rename = {'Ticker': 'Symbol'}

        s_and_p_500_df.rename(columns=s_and_p_500_column_rename, inplace=True)
        nasdaq_100_df.rename(columns=nasdaq_100_column_rename, inplace=True)

        dfs = [s_and_p_500_df, nasdaq_100_df, dow_30_df]
        index_df = pd.concat(dfs, ignore_index=True)

        # keep='first': 중복된 값 중 첫 번째로 나오는 레코드를 유지하고 나머지 중복 레코드는 제거
        index_df.drop_duplicates(subset=['Symbol'], keep='first', inplace=True)

        # 각 지수에 속한 종목을 1(True) 또는 0(False) 으로 표시하는 열 추가
        index_df['S&P500'] = index_df['Symbol'].isin(s_and_p_500_df['Symbol']).astype(int)
        index_df['NASDAQ100'] = index_df['Symbol'].isin(nasdaq_100_df['Symbol']).astype(int)
        index_df['DOW30'] = index_df['Symbol'].isin(dow_30_df['Symbol']).astype(int)

        # AWS GLUE 에서 콤마 문제를 해결하기 위해 일부 컬럼의 경우 ' -' 처리
        index_df['GICS Sub-Industry'] = index_df['GICS Sub-Industry'].str.replace(',', ' -')

        csv_file_path = f"{self.download_directory_path}{os.sep}" \
                        f"us_market_index_{target_date}.csv"

        index_df.to_csv(csv_file_path, index=False)

        # return index_df

    def get_download_zip_file_path_and_name(self, exchange_code):
        zip_file_name = f"{exchange_code}mst.cod.zip"
        zip_file_path = f"{self.download_directory_path}{os.sep}{zip_file_name}"

        return zip_file_path, zip_file_name

    def download_master_zip_file(self, zip_file_path, zip_file_name):
        download_zip_file_url = f"https://new.real.download.dws.co.kr/common/master/{zip_file_name}"
        urllib.request.urlretrieve(download_zip_file_url, zip_file_path)

    def unzip_master_zip_file(self, zip_file_path, zip_file_name):
        file_directory = self.download_directory_path
        us_market_zip_file = zipfile.ZipFile(zip_file_path)
        us_market_zip_file.extractall(file_directory)
        us_market_zip_file.close()

    def get_stock_exchange_data_df(self, exchange_code):
        cod_file_path = f"{self.download_directory_path}{os.sep}{exchange_code}mst.cod"
        columns = [
            'National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol',
            'Korea name', 'English name', 'Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)', 'currency',
            'float position', 'data type', 'base price', 'Bid order size', 'Ask order size',
            'market start time(HHMM)', 'market end time(HHMM)', 'DR 여부(Y/N)', 'DR 국가코드', '업종분류코드',
            '지수구성종목 존재 여부(0:구성종목없음,1:구성종목있음)', 'Tick size Type',
            '구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)',
            'Tick size type 상세'
        ]
        print(f"Downloading : {exchange_code}mst.cod file")
        stock_exchange_data_df = pd.read_table(cod_file_path, sep='\t', encoding='cp949')
        stock_exchange_data_df.columns = columns
        os.remove(cod_file_path)

        return stock_exchange_data_df

    def make_us_market_master_file(self, **kwargs):
        """
        # 나스닥, 뉴욕, 아멕스 : 'nas', 'nys', 'ams'
        # 상해, 상해지수, 심천, 심천지수, 도쿄, 홍콩, 하노이, 호치민 : 'shs', 'shi', 'szs', 'szi', 'tse', 'hks', 'hnx', 'hsx'
        """
        data_interval_end = kwargs["data_interval_end"]
        pdt_data_interval_end = convert_utc_to_pdt(data_interval_end)
        target_date = pdt_data_interval_end.strftime("%Y%m%d")

        # stock_exchange_name_list = ['nas', 'nys', 'ams', 'shs', 'shi', 'szs', 'szi', 'tse', 'hks', 'hnx', 'hsx']
        stock_exchange_name_list = ['nas', 'nys', 'ams']  # 순서 대로 나스닥, 뉴욕, 아멕스

        all_stock_exchange_df = pd.DataFrame()
        for index, stock_exchange_name in enumerate(stock_exchange_name_list):
            file_path, file_name = self.get_download_zip_file_path_and_name(stock_exchange_name)
            self.download_master_zip_file(file_path, file_name)
            self.unzip_master_zip_file(file_path, file_name)

            stock_exchange_data_df = self.get_stock_exchange_data_df(stock_exchange_name)
            all_stock_exchange_df = pd.concat([all_stock_exchange_df, stock_exchange_data_df], axis=0)

            os.remove(file_path)  # 파일 제거

        csv_file_path = f"{self.download_directory_path}{os.sep}" \
                        f"us_market_master_{target_date}.csv"
        all_stock_exchange_df.to_csv(csv_file_path, index=False)

    def make_target_stock_file(self, **kwargs):
        data_interval_end = kwargs["data_interval_end"]
        pdt_data_interval_end = convert_utc_to_pdt(data_interval_end)
        target_date = pdt_data_interval_end.strftime("%Y%m%d")

        # csv file list : us_market_index_{target_date}.csv, us_market_master_{target_date}.csv
        csv_index_file_path = f"{self.download_directory_path}{os.sep}" \
                              f"us_market_index_{target_date}.csv"
        csv_master_file_path = f"{self.download_directory_path}{os.sep}" \
                               f"us_market_master_{target_date}.csv"

        # TODO : 별도 파일로 관리 해야함
        additional_symbol_list = [
            'DIA', 'SPY', 'QQQ', 'IWM', 'IWO', 'VTV',  # 6
            'XLK', 'XLY', 'XLV', 'XLF', 'XLI', 'XLP', 'XLU', 'XLB', 'XLE', 'XLC', 'XLRE',  # 11
            'COWZ', 'HYG', 'HYBB', 'STIP', 'SCHD', 'SPLG', 'IHI', 'TLT', 'KMLM', 'MOAT',  # 10
            'EWY', 'EWJ',  # 2
            'IYT',  # 1
            'BROS', 'SLG', 'EPR', 'ZIP', 'SMCI', 'PLTR', 'CPNG'  # 7
        ]

        master_df = pd.read_csv(csv_master_file_path)
        index_df = pd.read_csv(csv_index_file_path)

        # preprocessing - B class stock - e.g. BRK.B - > BRK/B, 한투에서 받아들이는 코드가 '/' 기반
        index_df['Symbol'] = index_df['Symbol'].str.replace(r'-B', '/B')

        # preprocessing - 필요한 항목만 선택
        # 'Security type' : (1:Index,2:Stock,3:ETP(ETF),4:Warrant)
        print(master_df.columns.tolist())
        rename_column_dict ={
            'Exchange code': 'exchange_code',
            'Exchange name': 'exchange_name',
            'Korea name': 'korea_name',
            'English name': 'english_name',
            "Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)": "security_type"
        }

        master_df.rename(
            columns=rename_column_dict,
            inplace=True
        )
        target_column = [
            'Symbol', 'exchange_code', 'exchange_name',
            'korea_name', 'english_name', 'security_type'
        ]
        master_df = master_df[target_column]

        df_with_exchange_info = pd.merge(index_df, master_df, on='Symbol', how='left')
        symbol_exchange_info = master_df[master_df['Symbol'].isin(additional_symbol_list)]
        df_symbol_exchange = pd.concat(
            [df_with_exchange_info, symbol_exchange_info],
            ignore_index=True
        )

        # Preprocessing : Not exist - exchange code in master
        df_symbol_exchange.loc[df_symbol_exchange['Symbol'] == 'A', ['exchange_code', 'security_type']] = ['NYS', 2]

        us_market_stock_path = f"{self.download_directory_path}{os.sep}" \
                               f"us_market_stock_{target_date}.csv"

        df_symbol_exchange.to_csv(us_market_stock_path, index=False)


class UploadUSAMarketCodeFileToS3:
    """
    마켓 데이터를 S3로 보내는 코드 입니다. 보내는 이유는 지수 편입 종목의 편입 편출 추적 및 데이터 관리를 위해서 입니다.
    """
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
            token = file_name.split(".")[0].split("_")

            data_type = token[2]
            date_string = token[-1]  # date parsing

            print(f"date_string : {date_string} **********")
            year, month, day = date_string[0:4], date_string[4:6], date_string[6:]
            print(f"**** year : month, day = {year} : {month} : {day} *********")

            s3_path = f"{year}{os.sep}{month}{os.sep}{day}{os.sep}"
            s3_client.upload_file(file_path, self.bucket_name, f"{s3_path}{file_name}")
            print(f"Complete upload : {s3_path}{file_name}")

            delete_file = ["index", "master"]
            if data_type in delete_file:
                os.remove(file_path)

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
        pdt_data_interval_end = convert_utc_to_pdt(data_interval_end)
        target_date = pdt_data_interval_end.strftime("%Y%m%d")

        slack_message = f"""
        **결과 요약**
        - DAG 작업: {dag_id}
        - 작업 날짜 : {target_date}
        - kst_data_interval_end : {pdt_data_interval_end}
        """

        task_instance = kwargs['ti']
        task_instance.xcom_push(
            key="slack_message",
            value=slack_message
        )


download_usa_market_data = DownloadUSAMarketData()
upload_csv_to_s3 = UploadUSAMarketCodeFileToS3()

with DAG(
    dag_id="us_market_stock_code_download_csv_and_upload_to_s3",
    start_date=datetime(2023, 9, 25, 9),
    schedule_interval='15 9 * * 2-6',  # 한국 기준 화 - 토, 이유는 마스터 파일 갱신 시간이 17:30, 17:55로 안내됨
    catchup=False,
    tags=["US", "Download", "Index", "Stock"]
) as dag:
    make_us_market_master_file_task = PythonOperator(
        task_id="make_us_market_master_file",
        python_callable=download_usa_market_data.make_us_market_master_file,
        provide_context=True
    )
    download_stock_data_for_index_task = PythonOperator(
        task_id="download_stock_data_for_index",
        python_callable=download_usa_market_data.download_stock_data_for_index,
        provide_context=True
    )
    make_target_stock_file_task = PythonOperator(
        task_id="make_target_stock_file",
        python_callable=download_usa_market_data.make_target_stock_file,
        provide_context=True
    )
    done_us_market_data_task = EmptyOperator(
        task_id="done_us_market_data",
        trigger_rule="none_failed"
    )
    upload_csv_to_s3_task = PythonOperator(
        task_id="upload_csv_to_s3",
        python_callable=upload_csv_to_s3.upload_csv_to_s3,
        provide_context=True
    )
    done_upload_task = EmptyOperator(
        task_id="done_upload",
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

    make_us_market_master_file_task >> download_stock_data_for_index_task >> make_target_stock_file_task
    make_target_stock_file_task >> done_us_market_data_task >> upload_csv_to_s3_task >> done_upload_task
    done_upload_task >> make_slack_message_task >> slack_notification_task