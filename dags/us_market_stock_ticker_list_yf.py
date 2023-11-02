import configparser
import os
import pandas as pd
import pytz
import requests
import shutil
import yfinance as yf
import sys
sys.path.append('/opt/airflow/')

from utils.market_open_status import GetUSAMarketOpenStatus
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator


def get_pdt_date_from_utc_time(pendulum_utc_datetime):
    # utc_time : pendulum.datetime - kwargs["data_interval_end"] or kwargs["data_interval_start"]
    pdt_time = pendulum_utc_datetime.in_timezone('America/Los_Angeles')
    pdt_date = pdt_time.to_date_string().replace('-', '')  # "%Y%m%d"

    return pdt_date


def get_data_directory_path():
    target_directory_name = "airflow/data"
    # current_directory_path = os.getcwd()
    parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
    target_directory_path = os.path.join(parent_directory_path, target_directory_name)

    return target_directory_path


def download_stock_index_data_from_wiki():
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

    # B class stock ticker(E.g. BRK.B -> BRK-B)
    index_df['Symbol'] = index_df['Symbol'].str.replace(r'\.B', '-B', regex=True)

    # 작업 편의상 symbol 을 다시 ticker 로 변경
    index_df.rename(columns={'Symbol': 'Ticker'}, inplace=True)

    # 컬럼명을 소문자로 변경
    index_df.columns = index_df.columns.str.lower()

    return index_df


def add_stock_ticker_list(basic_list):
    additional_symbol_list = [
        'DIA', 'SPY', 'QQQ', 'IWM', 'IWO', 'VTV',  # 6
        'XLK', 'XLY', 'XLV', 'XLF', 'XLI', 'XLP', 'XLU', 'XLB', 'XLE', 'XLC', 'XLRE',  # 11
        'COWZ', 'HYG', 'HYBB', 'STIP', 'SCHD', 'SPLG', 'IHI', 'TLT', 'KMLM', 'MOAT',  # 10
        'EWY', 'EWJ',  # 2
        'IYT',  # 1
        'BROS', 'SLG', 'EPR', 'ZIP', 'SMCI', 'PLTR', 'CPNG',  # 7
        '^GSPC', '^DJI', '^IXIC', '^RUT', '^TNX'
    ]

    add_result = basic_list + additional_symbol_list
    unique_ticker_list = list(dict.fromkeys(add_result).keys())  # Python 3.6+

    return unique_ticker_list


def get_ticker_list_from_exchange():
    """
    yf 라이브러리 이슈로 현재는 사용 중지 상태, 이슈 해결 후 사용 예정 - 20231031
    """
    nasdaq_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt"
    amex_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_tickers.txt"
    nyse_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_tickers.txt"
    all_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    exchange_url_list = [nasdaq_url, amex_url, nyse_url, all_url]

    all_ticker_list = []

    for exchange_url in exchange_url_list:
        response = requests.get(exchange_url)

        if response.status_code == 200:
            data_lines = response.text.split('\n')
            # 빈 줄 제거
            data_lines = [line.strip() for line in data_lines if line.strip()]
            all_ticker_list += data_lines
        else:
            print("Failed to retrieve data from the URL.")

    unique_ticker_list = list(dict.fromkeys(all_ticker_list).keys())

    return all_ticker_list


def get_stock_data(ticker):
    """yf 이슈로 info 를 가져오는데 실패하고 있음, 수정 예정 - 20231031"""
    response_res = yf.Ticker(ticker)
    stock_yf_basic_info = response_res.basic_info
    quote_type = stock_yf_basic_info.quote_type

    if quote_type == 'EQUITY':
        marketCap_value = stock_yf_basic_info.market_cap
        quote_type_lower = quote_type.lower()

        stock_simple_info_dict ={
            'ticker': ticker,
            'quote_type': quote_type_lower,
            'asset_size': marketCap_value,
        }

        return stock_simple_info_dict

    elif quote_type == 'ETF':
        quote_type_lower = quote_type.lower()

        stock_simple_info_dict ={
            'ticker': ticker,
            'quote_type': quote_type_lower,
            'asset_size': None
        }

        return stock_simple_info_dict

    elif quote_type == 'INDEX':
        quote_type_lower = quote_type.lower()

        stock_simple_info_dict ={
            'ticker': ticker,
            'quote_type': quote_type,
            'asset_size': None
        }

        return stock_simple_info_dict
    else:
        print(f"Information is not exist in YF : {ticker}, "
              f"stock_yf_basic_info['quoteType'] : {stock_yf_basic_info['quoteType']}")

        raise ValueError


def get_stock_ticker_df_csv_files(target_date):
    stock_index_wiki_df = download_stock_index_data_from_wiki()
    stock_index_ticker_list = stock_index_wiki_df['ticker'].tolist()

    # stock_ticker_list_from_exchange = get_ticker_list_from_exchange()
    # stock_index_ticker_list += stock_ticker_list_from_exchange
    stock_ticker_list = add_stock_ticker_list(stock_index_ticker_list)

    all_ticker_info_list = []
    # stock_ticker_list = ['SPY', 'QQQ', 'AAPL']
    # print(stock_ticker_list)

    # 시가총액이 빠짐

    for idx, ticker in enumerate(stock_ticker_list):
        try:
            result = get_stock_data(ticker)
            all_ticker_info_list.append(result)
        except:
            print(f"idx : {idx}, Data is not exist : {ticker}")
            n = 0
            while n < 5:
                try:
                    result = get_stock_data(ticker)
                    all_ticker_info_list.append(result)
                    print(f"Success - idx : {idx}, ticker : {ticker}, n : {n}")
                    break

                except Exception as e:
                    print(f"Error - idx : {idx}, ticker : {ticker}, e : {e}, n : {n}")
                    n += 1

                    if n >= 5:
                        print(f"Fail - idx : {idx}, ticker : {ticker}, error : {e}, n : {n}")
                        raise ValueError

        if idx % 10 == 0:
            print(f'idx : {idx}, ticker : {ticker} .... ing')
    #         break
    #     if idx == 100:
    #         break
    all_ticker_df = pd.DataFrame.from_dict(all_ticker_info_list)
    all_ticker_df = all_ticker_df.merge(
        stock_index_wiki_df[['ticker', 's&p500', 'nasdaq100', 'dow30']],
        on='ticker',
        how='left'
    )

    all_ticker_df['date'] = target_date

    data_directory_path = get_data_directory_path()
    directory_path = f"{data_directory_path}{os.sep}{target_date}"
    print(directory_path)
    print(all_ticker_df)
    os.makedirs(directory_path, exist_ok=True)
    all_ticker_file_name = f"{directory_path}{os.sep}us_market_ticker_{target_date}_{target_date}.csv"
    all_ticker_df.to_csv(all_ticker_file_name, index=False)



def remove_files_in_directory(directory_path):
    for item in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item)

        if os.path.isfile(item_path):
            # 파일일 경우 제거
            os.remove(item_path)
        elif os.path.isdir(item_path):
            # 디렉터리일 경우 shutil.rmtree()를 사용하여 재귀적으로 제거
            shutil.rmtree(item_path)


def get_slack_channel_info():
    configuration_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
    config_file_path = os.path.join(
        configuration_directory,
        "airflow/config",
        "config.ini"
    )

    config_object = configparser.ConfigParser()
    config_object.read(config_file_path)

    slack_message_token = config_object.get('SLACK_CONFIG', 'channel_access_token')
    channel_name = config_object.get('SLACK_CONFIG', 'channel_name')
    slack_channel_name = f"#{channel_name}"

    slack_channel_info = {
        'channel': slack_channel_name,
        'token': slack_message_token
    }

    return slack_channel_info


with DAG(
    dag_id="us_market_yf_ticker_list",
    start_date=datetime(2023, 10, 26, 9),
    schedule_interval='30 23 * * 1-5',  # 한국 시간 아침 9시
    # default_args=default_args,
    catchup=False
) as dag:
    def get_market_open_status(**kwargs):
        check_market_open_status = GetUSAMarketOpenStatus()

        data_interval_end = kwargs["data_interval_end"]
        usa_data_interval_end = data_interval_end.in_timezone('America/Los_Angeles')
        target_date = usa_data_interval_end.to_date_string().replace('-', '')  # "%Y%m%d"

        print(f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
              f' kwargs["data_interval_end"].strftime("%Y%m%d") : '
              f' {kwargs["data_interval_end"].strftime("%Y%m%d")}'
              f' usa_data_interval_end : {usa_data_interval_end}'
              f' target_date : {target_date}')

        open_yn_result = check_market_open_status.get_us_market_status(target_date)
        # open_yn_result = "Y"  # for testing
        kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)

    def check_open_status(**kwargs):
        open_yn_result = kwargs["ti"].xcom_pull(
            key="open_yn_result",
            task_ids="get_market_open_status"
        )
        # open_yn_result = "N"

        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_opened_task"


    def make_slack_message(**kwargs):
        dag_id = kwargs['dag'].dag_id
        data_interval_end = kwargs["data_interval_end"]
        target_date = get_pdt_date_from_utc_time(data_interval_end)

        slack_message = f"""
        **결과 요약**
        - DAG 작업: {dag_id}
        - 작업 날짜 : {target_date}
        """

        task_instance = kwargs['ti']
        task_instance.xcom_push(
            key="slack_message",
            value=slack_message
        )

    def upload_csv_to_s3_bucket():
        bucket_name = "your-bucket-name"
        data_directory_path = get_data_directory_path()

        s3_hook = S3Hook('s3_conn')

        for root, dirs, files in os.walk(data_directory_path):
            for file in files:
                if file.lower().endswith('.csv'):
                    local_file_path = os.path.join(root, file)

                    s3_key = os.path.relpath(local_file_path, data_directory_path)
                    s3_hook.load_file(
                        filename=local_file_path,
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True
                    )

        remove_files_in_directory(data_directory_path)

    def download_stock_ticker_csv_file(**kwargs):
        data_interval_end = kwargs["data_interval_end"]
        target_date = get_pdt_date_from_utc_time(data_interval_end)
        get_stock_ticker_df_csv_files(target_date)


    get_market_open_status_task = PythonOperator(
        task_id="get_market_open_status",
        python_callable=get_market_open_status,
        provide_context=True
    )

    check_open_status_task = BranchPythonOperator(
        task_id="check_open_status",
        python_callable=check_open_status,
        provide_context=True
    )

    download_stock_ticker_data_csv_task = PythonOperator(
        task_id="download_stock_ticker_data_csv",
        python_callable=download_stock_ticker_csv_file,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_opened_task = EmptyOperator(task_id="market_opened_task")

    upload_csv_to_s3_bucket_task = PythonOperator(
        task_id="upload_csv_to_s3_bucket",
        python_callable=upload_csv_to_s3_bucket
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    make_slack_message_task = PythonOperator(
        task_id="make_slack_message",
        python_callable=make_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token=get_slack_channel_info()['token'],
        channel=get_slack_channel_info()['channel'],
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    get_market_open_status_task >> check_open_status_task
    check_open_status_task >> market_closed_task >> done_task
    check_open_status_task >> market_opened_task
    market_opened_task >> download_stock_ticker_data_csv_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task