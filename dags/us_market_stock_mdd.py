import configparser
import empyrical as ep
import numpy as np
import os
import pandas as pd
import pytz
import pendulum
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

def make_stock_directory(ticker):
    base_directory = get_data_directory_path()
    target_directory_path = f"{base_directory}{os.sep}{ticker}"
    os.makedirs(target_directory_path, exist_ok=True)

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


def get_max_drawdown_underwater(underwater):
    """
    Determines peak, valley, and recovery dates given an 'underwater'
    DataFrame.
    An underwater DataFrame is a DataFrame that has precomputed
    rolling drawdown.
    Parameters
    ----------
    underwater : pd.Series
       Underwater returns (rolling drawdown) of a strategy.
    Returns
    -------
    peak : datetime
        The maximum drawdown's peak.
    valley : datetime
        The maximum drawdown's valley.
    recovery : datetime
        The maximum drawdown's recovery.
    """

    valley = underwater.idxmin()  # end of the period
    # Find first 0
    peak = underwater[:valley][underwater[:valley] == 0].index[-1]
    # Find last 0
    try:
        recovery = underwater[valley:][underwater[valley:] == 0].index[0]
    except IndexError:
        recovery = np.nan  # drawdown not recovered
    return peak, valley, recovery


def get_top_drawdowns(returns, top=10):
    """
    Finds top drawdowns, sorted by drawdown amount.
    Parameters
    ----------
    returns : pd.Series
        Daily returns of the strategy, noncumulative.
         - See full explanation in tears.create_full_tear_sheet.
    top : int, optional
        The amount of top drawdowns to find (default 10).
    Returns
    -------
    drawdowns : list
        List of drawdown peaks, valleys, and recoveries. See get_max_drawdown.
    """

    returns = returns.copy()
    df_cum = ep.cum_returns(returns, 1.0)
    running_max = np.maximum.accumulate(df_cum)
    underwater = df_cum / running_max - 1

    drawdowns = []
    for _ in range(top):
        peak, valley, recovery = get_max_drawdown_underwater(underwater)
        # Slice out draw-down period
        if not pd.isnull(recovery):
            underwater.drop(
                underwater[peak:recovery].index[1:-1], inplace=True
            )
        else:
            # drawdown has not ended yet
            underwater = underwater.loc[:peak]

        drawdowns.append((peak, valley, recovery))
        if (
                (len(returns) == 0)
                or (len(underwater) == 0)
                or (np.min(underwater) == 0)
        ):
            break

    return drawdowns


def gen_drawdown_table(returns, top=10):
    """
    Places top drawdowns in a table.
    Parameters
    ----------
    returns : pd.Series
        Daily returns of the strategy, noncumulative.
         - See full explanation in tears.create_full_tear_sheet.
    top : int, optional
        The amount of top drawdowns to find (default 10).
    Returns
    -------
    df_drawdowns : pd.DataFrame
        Information about top drawdowns.
    """

    df_cum = ep.cum_returns(returns, 1.0)
    drawdown_periods = get_top_drawdowns(returns, top=top)
    df_drawdowns = pd.DataFrame(
        index=list(range(top)),
        columns=[
            "Net drawdown in %",
            "Peak date",
            "Valley date",
            "Recovery date",
            "Duration",
        ],
    )

    for i, (peak, valley, recovery) in enumerate(drawdown_periods):
        if pd.isnull(recovery):
            df_drawdowns.loc[i, "Duration"] = np.nan
        else:
            df_drawdowns.loc[i, "Duration"] = len(
                pd.date_range(peak, recovery, freq="B")
            )
        df_drawdowns.loc[i, "Peak date"] = peak.to_pydatetime().strftime("%Y-%m-%d")
        df_drawdowns.loc[i, "Valley date"] = valley.to_pydatetime().strftime("%Y-%m-%d")
        if isinstance(recovery, float):
            df_drawdowns.loc[i, "Recovery date"] = recovery
        else:
            df_drawdowns.loc[
                i, "Recovery date"
            ] = recovery.to_pydatetime().strftime("%Y-%m-%d")
        # df_drawdowns.loc[i, "Net drawdown in %"] = ((df_cum.loc[peak] - df_cum.loc[valley]) / df_cum.loc[peak])
        df_drawdowns.loc[i, "Net drawdown in %"] = ((df_cum.loc[peak] - df_cum.loc[valley]) / df_cum.loc[peak]) * 100

    df_drawdowns["Peak date"] = pd.to_datetime(df_drawdowns["Peak date"])
    df_drawdowns["Valley date"] = pd.to_datetime(df_drawdowns["Valley date"])
    df_drawdowns["Recovery date"] = pd.to_datetime(
        df_drawdowns["Recovery date"]
    )

    return df_drawdowns


def show_worst_drawdown_periods(returns, top=5):
    """
    Prints information about the worst drawdown periods.
    Prints peak dates, valley dates, recovery dates, and net
    drawdowns.
    Parameters
    ----------
    returns : pd.Series
        Daily returns of the strategy, noncumulative.
         - See full explanation in tears.create_full_tear_sheet.
    top : int, optional
        Amount of top drawdowns periods to plot (default 5).
    """

    drawdown_df = gen_drawdown_table(returns, top=top)
    drawdown_df.sort_values("Net drawdown in %", ascending=False, inplace=True)
    drawdown_df.dropna(subset=['Net drawdown in %'], inplace=True)
    drawdown_df.reset_index(inplace=True)

    converted_column_list = {
        'index': 'worst_drawdown_periods',
        'Net drawdown in %': 'net_drawdown',
        'Peak date': 'peak_date',
        'Valley date': 'valley_date',
        'Recovery date': 'recovery_date',
        'Duration': 'duration'
    }
    drawdown_df.rename(columns=converted_column_list, inplace=True)

    return drawdown_df


def get_stock_close_series_data(ticker):
    yf_ticker = yf.Ticker(ticker)
    yf_ticker_max_history = yf_ticker.history(period='max', auto_adjust=False)
    result_close_series_data = yf_ticker_max_history['Close'].pct_change()

    return result_close_series_data


def make_maximum_drawdown_df(data, top=300):
    mdd_period_df = show_worst_drawdown_periods(data, top=top)

    return mdd_period_df


with DAG(
    dag_id="us_market_yf_maximum_drawdown",
    start_date=datetime(2023, 10, 26, 9),
    schedule_interval='0 1 * * 6',  # 한국 시간 아침 10시
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
        open_yn_result = kwargs["ti"].xcom_pull(key="open_yn_result", task_ids="get_market_open_status")
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
                        replace=True  # mdd 여서 덮어씌워도 무방함
                    )

        remove_files_in_directory(data_directory_path)

    def get_stock_ticker_df_csv_files(**kwargs):
        data_interval_end = kwargs["data_interval_end"]
        target_date = get_pdt_date_from_utc_time(data_interval_end)

        stock_index_wiki_df = download_stock_index_data_from_wiki()
        stock_index_ticker_list = stock_index_wiki_df['ticker'].tolist()
        stock_ticker_list = add_stock_ticker_list(stock_index_ticker_list)

        stock_ticker_list = ['AAPL', 'SPY', '^GSPC']  # test

        for idx, ticker in enumerate(stock_ticker_list):
            stock_close_series_data = get_stock_close_series_data(ticker)
            maximum_drawdown_df = make_maximum_drawdown_df(stock_close_series_data)

            target_directory_path = make_stock_directory(ticker)  # 종목별로 디렉터리 만들기
            target_file_path = f"{target_directory_path}{os.sep}{ticker}_mdd.csv"
            maximum_drawdown_df.to_csv(target_file_path, index=False)


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

    get_stock_ticker_df_csv_files_task = PythonOperator(
        task_id="get_stock_ticker_df_csv_files",
        python_callable=get_stock_ticker_df_csv_files,
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
    market_opened_task >> get_stock_ticker_df_csv_files_task >> upload_csv_to_s3_bucket_task
    upload_csv_to_s3_bucket_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task