import os
import pandas as pd
import requests
import pytz
import glob
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from config_generate import GetUSAMarketOpenStatus
from datetime import datetime


class GetDailyStockData(GetUSAMarketOpenStatus):
    def __init__(self):
        super().__init__()
        self.target_directory_path = self.check_stock_data_directory()
        self.target_stock_code_dict = {}

    def check_stock_data_directory(self):
        target_directory_name = "airflow/data"
        parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
        target_directory_path = os.path.join(parent_directory_path, target_directory_name)
        os.makedirs(target_directory_path, exist_ok=True)

        return target_directory_path

    def preprocessing_csv_to_dict(self, file_path):
        origin_code_file_df = pd.read_csv(file_path, dtype=str)
        result_dict = origin_code_file_df.groupby('Symbol').apply(
            lambda x: x.drop('Symbol', axis=1).to_dict('records')
        ).to_dict()

        print(f"pre part result_dict : {result_dict}")

        return result_dict

    def read_stock_code_data(self):
        code_csv_file_list = glob.glob(f"{self.target_directory_path}{os.sep}*.csv")
        delimiter = os.sep
        print(f"code_csv_file_list : {code_csv_file_list}")

        for idx, file_path in enumerate(code_csv_file_list):
            result = self.preprocessing_csv_to_dict(file_path)
            self.target_stock_code_dict.update(result)

            os.remove(file_path)

        return self.target_stock_code_dict  # TODO : 이 부분 수정 필요

    def get_stock_data(self, exchange_code, ticker, target_date):
        """
        거래소코드
        - NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스
        - HKS: 홍콩, TSE: 도쿄, SHS: 상해, SZS: 심천, SHI: 상해지수, SZI: 심천지수
        - HSX: 호치민, HNX: 하노이
        - BAY: 뉴욕(주간), BAQ: 나스닥(주간), BAA: 아멕스(주간)
        """
        # key, secret_key, token = get_config()
        url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
        path = "/uapi/overseas-price/v1/quotations/dailyprice"
        url = f"{url_base}/{path}"

        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.key,
            "appSecret": self.secret_key,
            "tr_id": "HHDFS76240000"
        }

        params = {
            "AUTH": "",
            "EXCD": exchange_code,  # 거래소코드
            "SYMB": ticker,  # 종목코드 (ex. TSLA)
            "GUBN": "0",  # 0: 일, 1: 주, 2: 월
            "BYMD": target_date,  # 조회 날짜 : YYYYMMDD
            "MODP": "1",  # 수정주가반영여부, 0: 미반영, 1: 반영
            "fid_org_adj_prc": "0",
        }

        res = requests.get(url, headers=headers, params=params)

        return res.json()

    def convert_daily_data(self, data_result):
        return pd.DataFrame([data_result])

    def convert_json_to_csv(self, symbol, stock_json, stock_info):
        stock_rsym = stock_json['output1']['rsym']
        # stock_ticker = stock_rsym[4:]  # "D+시장구분(3자리)+종목코드 : DNASAAPL : D+NAS(나스닥)+AAPL(애플)"
        # stock_exchange_code = stock_rsym[1:4]

        output_one = stock_json['output1']  # output1 전체
        output_two = stock_json['output2'][0]  # output2 - 1일 데이터(원래는 100일 데이터 한 번에 리턴)

        output_one_df = self.convert_daily_data(output_one)
        output_two_df = self.convert_daily_data(output_two)

        daily_stock_data_df = pd.concat([output_two_df, output_one_df], axis=1)
        daily_stock_data_df['xymd'] = daily_stock_data_df['xymd'].apply(
            lambda x: pd.to_datetime(str(x), format='%Y%m%d')
        )

        daily_stock_data_df['symbol'] = symbol
        daily_stock_data_df['DOW30'] = str(stock_info[0]['DOW30'])
        daily_stock_data_df['NAS100'] = str(stock_info[0]['NASDAQ100'])
        daily_stock_data_df['S&P500'] = str(stock_info[0]['S&P500'])
        daily_stock_data_df['korea_name'] = stock_info[0]['korea_name']
        daily_stock_data_df['english_name'] = stock_info[0]['english_name']
        daily_stock_data_df['security_type'] = stock_info[0]['security_type']
        daily_stock_data_df['exchange_code'] = stock_info[0]['exchange_code']
        daily_stock_data_df['exchange_name'] = stock_info[0]['exchange_name']
        daily_stock_data_df['GICS Sector'] = stock_info[0]['GICS Sector']
        daily_stock_data_df['GICS Sub-Industry'] = stock_info[0]['GICS Sub-Industry']
        daily_stock_data_df['Company'] = stock_info[0]['Company']

        return daily_stock_data_df

    def concat_and_save_stock_dataframe(self, stock_info_dict, target_date):
        error_count = 0
        dfs = []

        for symbol, stock_info in stock_info_dict.items():
            print(f"Download {symbol} - data - {stock_info}, Date : {target_date}")
            stock_ticker = symbol
            stock_exchange_code = stock_info[0]['exchange_code']
            stock_json = self.get_stock_data(stock_exchange_code, stock_ticker, target_date)

            try:
                result_df = self.convert_json_to_csv(symbol, stock_json, stock_info)
                dfs.append(result_df)

            except Exception as e:
                print(f"error : {e}, symbol : {symbol}")
                error_count += 1

            print("--------------------------------------------------")
            time.sleep(0.1)

        print(f"error count : {error_count}")
        daily_stock_data_df = pd.concat(dfs, ignore_index=True)
        daily_stock_data_df.rename(columns={'clos': 'close'}, inplace=True)

        csv_file_name = f"{self.target_directory_path}{os.sep}" \
                        f"us_market_{target_date}_{target_date}.csv"
        daily_stock_data_df.to_csv(csv_file_name, index=False)

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


get_daily_stock_data = GetDailyStockData()


def convert_utc_to_pdt(utc_time):
    try:
        pdt_timezone = pytz.timezone('America/Los_Angeles')
        pdt_time = utc_time.replace(tzinfo=pytz.utc).astimezone(pdt_timezone)

        return pdt_time
    except Exception as e:
        print(f"Error during timezone conversion : {e}")

        raise ValueError("Error during timezone conversion") from e


with DAG(
    dag_id="us_market_stock_daily_price_download",
    start_date=datetime(2023, 9, 25, 9),
    schedule_interval='30 9 * * 2-6',
    # default_args=default_args,
    catchup=False
) as dag:
    def get_market_open_status(**kwargs):
        check_market_open_status = GetUSAMarketOpenStatus()

        data_interval_end = kwargs["data_interval_end"]
        usa_data_interval_end = convert_utc_to_pdt(data_interval_end)
        target_date = usa_data_interval_end.strftime("%Y%m%d")

        print(f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
              f' kwargs["data_interval_end"].strftime("%Y%m%d") : '
              f' {kwargs["data_interval_end"].strftime("%Y%m%d")}'
              f' usa_data_interval_end : {usa_data_interval_end}'
              f' target_date : {target_date}')

        open_yn_result = check_market_open_status.get_us_market_status(target_date)
        open_yn_result = "Y"  # for testing
        kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)

    def check_open_status(**kwargs):
        open_yn_result = kwargs["ti"].xcom_pull(key="open_yn_result", task_ids="get_market_open_status")
        # open_yn_result = "N"

        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_open_task"

    def read_and_pass_stock_code_data(**kwargs):
        # TODO : 변수명 적절한 이름으로 리팩토링 하기
        stock_code_data = get_daily_stock_data.read_stock_code_data()
        kwargs["ti"].xcom_push(key="stock_code_data", value=stock_code_data)

        return stock_code_data

    def download_stock_data_csv(**kwargs):
        stock_info_dict = kwargs["ti"].xcom_pull(
            key="stock_code_data", task_ids='read_and_pass_stock_code'
        )

        data_interval_end = kwargs["data_interval_end"]
        usa_data_interval_end = convert_utc_to_pdt(data_interval_end)
        target_date = usa_data_interval_end.strftime("%Y%m%d")

        print(f"stock_code_dict from xcom_pull : {stock_info_dict}")
        get_daily_stock_data.concat_and_save_stock_dataframe(stock_info_dict, target_date)

    get_market_open_status = PythonOperator(
        task_id="get_market_open_status",
        python_callable=get_market_open_status,
        provide_context=True
    )

    check_open_status = BranchPythonOperator(
        task_id="check_open_status",
        python_callable=check_open_status,
        provide_context=True
    )

    read_and_pass_stock_code_task = PythonOperator(
        task_id="read_and_pass_stock_code",
        python_callable=read_and_pass_stock_code_data,
        provide_context=True
    )

    download_stock_data_csv_task = PythonOperator(
        task_id="download_stock_data_csv",
        python_callable=download_stock_data_csv,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_open_task = EmptyOperator(task_id="market_open_task")

    make_slack_message_task = PythonOperator(
        task_id="make_slack_message",
        python_callable=get_daily_stock_data.make_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token=get_daily_stock_data.get_slack_message_token(),
        channel=get_daily_stock_data.get_slack_channel_name(),
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    get_market_open_status >> check_open_status
    check_open_status >> market_closed_task >> done_task
    check_open_status >> market_open_task
    market_open_task >> read_and_pass_stock_code_task >> download_stock_data_csv_task
    download_stock_data_csv_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task