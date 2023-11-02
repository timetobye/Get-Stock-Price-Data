import argparse
import os
import pandas as pd
import subprocess
import shutil
import yfinance as yf


def make_history_data(ticker, period=None, start_date=None, end_date=None):
    ticker = ticker.upper()
    stock = yf.Ticker(ticker)

    if period is not None:
        stock_history_df = stock.history(period=period, auto_adjust = False)
    elif start_date is None and end_date is None:
        stock_history_df = stock.history(start='max', auto_adjust=False)
    elif end_date is None:
        stock_history_df = stock.history(start=start_date, auto_adjust=False)
    elif start_date is None:
        stock_history_df = stock.history(start='1993-01-01', end=end_date, auto_adjust=False)
    else:
        stock_history_df = stock.history(start=start_date, end=end_date, auto_adjust=False)

    stock_history_metadata = stock.history_metadata
    stock_type = stock_history_metadata.get('instrumentType', None)
    if stock_type is None:
        print(f"ticker : {ticker}, Stock type is None")

    # preprocessing part
    stock_history_df.reset_index(inplace=True)
    stock_history_df.columns = stock_history_df.columns.str.lower()
    stock_history_df.columns = stock_history_df.columns.str.replace(' ', '_')

    round_columns = ['open', 'high', 'low', 'close', 'adj_close']
    stock_history_df[round_columns] = stock_history_df[round_columns].round(2)

    stock_history_df['dividends'] = stock_history_df['dividends'].round(3)

    stock_history_df['date'] = pd.to_datetime(stock_history_df['date'], format='%Y-%m-%d %H:%M:%S-%z')
    stock_history_df['date'] = stock_history_df['date'].dt.strftime('%Y-%m-%d')

    if 'capital_gains' in stock_history_df.columns:
        # 'capital_gains' 컬럼이 존재하면 해당 컬럼 제거
        stock_history_df = stock_history_df.drop('capital_gains', axis=1)

    # Ticker 컬럼 추가
    stock_history_df['ticker'] = ticker

    # Type 컬럼 추가
    if stock_type:
        stock_history_df['stock_type'] = stock_type.lower()
    else:
        stock_history_df['stock_type'] = None

    return stock_history_df


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
    #     csv_file_path = f"{self.download_directory_path}{os.sep}" \
    #                     f"us_market_index_{target_date}.csv"

    #     index_df.to_csv(csv_file_path, index=False)

    return index_df


def make_stock_ticker_list(base_list):
    additional_symbol_list = [
        'DIA', 'SPY', 'QQQ', 'IWM', 'IWO', 'VTV',  # 6
        'XLK', 'XLY', 'XLV', 'XLF', 'XLI', 'XLP', 'XLU', 'XLB', 'XLE', 'XLC', 'XLRE',  # 11
        'COWZ', 'HYG', 'HYBB', 'STIP', 'SCHD', 'SPLG', 'IHI', 'TLT', 'KMLM', 'MOAT',  # 10
        'EWY', 'EWJ',  # 2
        'IYT',  # 1
        'BROS', 'SLG', 'EPR', 'ZIP', 'SMCI', 'PLTR', 'CPNG',  # 7
        '^GSPC', '^DJI', '^IXIC', '^RUT', '^TNX'
    ]

    add_result = base_list + additional_symbol_list
    unique_ticker_list = list(dict.fromkeys(add_result).keys())  # Python 3.6+

    return unique_ticker_list


def get_data_directory_path():
    target_directory_name = "data"
    # current_directory_path = os.getcwd()
    parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
    target_directory_path = os.path.join(parent_directory_path, target_directory_name)

    return target_directory_path


def make_stock_directory(ticker):
    base_directory = get_data_directory_path()
    target_directory_path = f"{base_directory}{os.sep}{ticker}"
    os.makedirs(target_directory_path, exist_ok=True)

    return target_directory_path


def make_stock_csv_file(ticker, period, start_date, end_date):
    ticker_history_df = make_history_data(ticker, period, start_date, end_date)
    start_date, end_date = ticker_history_df['date'].min(), ticker_history_df['date'].max()
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')

    ticker_history_df = ticker_history_df.merge(
        stock_index_wiki_df[['ticker', 's&p500', 'nasdaq100', 'dow30']],
        on='ticker',
        how='left'
    )

    target_directory_path = make_stock_directory(ticker)  # 종목별로 디렉터리 만들기
    target_file_path = f"{target_directory_path}{os.sep}{ticker}_{start_date}_{end_date}.csv"
    ticker_history_df.to_csv(target_file_path, index=False)

    # return ticker_history_df


def remove_files_in_directory(directory_path):
    for item in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item)

        if os.path.isfile(item_path):
            # 파일일 경우 제거
            os.remove(item_path)
        elif os.path.isdir(item_path):
            # 디렉터리일 경우 shutil.rmtree()를 사용하여 재귀적으로 제거
            shutil.rmtree(item_path)


if __name__ == "__main__":
    """
    # 지정된 종목을 일정 기간으로 지정해서 다운로드 하는 경우 - 일 단위로 받음
    python3 backfill_us_market_stock_daily_price_day_yf_ticker.py -p max
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--period",
        "-p",
        type=str,
        default=None,
        help="Input Period : only use max",
        required=False
    )

    arg_parser.add_argument(
        "--start_date",
        "-s",
        type=str,
        default=None,
        help="Input Start Date : EX 2023-01-01",
        required=False
    )

    arg_parser.add_argument(
        "--end_date",
        "-e",
        type=str,
        default=None,
        help="Input End Date : EX 2023-01-31",
        required=False
    )

    args = arg_parser.parse_args()
    period = args.period
    start_date = args.start_date
    end_date = args.end_date

    stock_index_wiki_df = download_stock_index_data_from_wiki()
    stock_index_ticker_list = stock_index_wiki_df['ticker'].tolist()
    stock_ticker_list = make_stock_ticker_list(stock_index_ticker_list)

    data_directory_path = get_data_directory_path()

    print(stock_ticker_list[0:10])
    dataframes = []  # 모든 종목 - 모든 기간

    # Sample Test
    # stock_ticker_list = ['SPY', 'QQQ', 'AAPL']
    # stock_ticker_list = ['SPY']
    # print(stock_ticker_list)

    for idx, ticker in enumerate(stock_ticker_list):
        try:
            make_stock_csv_file(ticker, period, start_date, end_date)
        except Exception as e:
            print(f"idx : {idx}, ticker : {ticker}, error : {e}")
            # yf 가 종종 에러가 나는 경우가 발생하여, loop 처리로 대응
            n = 0
            while n < 5:
                try:
                    make_stock_csv_file(ticker, period, start_date, end_date)
                    print(f"Success - idx : {idx}, ticker : {ticker}, n : {n}")
                    break

                except Exception as e:
                    print(f"Error - idx : {idx}, ticker : {ticker}, e : {e}, n : {n}")
                    n += 1

                    if n >= 5:
                        print(f"Fail - idx : {idx}, ticker : {ticker}, error : {e}, n : {n}")
                        raise ValueError

        if (idx % 100) == 0:
            print(f"idx : {idx}, ticker : {ticker}")

    # s3 upload
    bucket_name = "your-bucket-name"

    if period is not None:
        # max 로 옵션을 받을 때, 버킷을 비운 다음에 전체 기간에 대한 데이터를 업로드 하기 위한 밑작업 입니다.
        for rm_ticker in stock_ticker_list:
            rm_s3_directory_command = f"aws s3 rm s3://{bucket_name}/{rm_ticker}/ --recursive"
            rm_result = subprocess.run(rm_s3_directory_command, shell=True, check=True, text=True)

            if rm_result.returncode == 0:
                print(f"Success : Remove S3 {rm_ticker} directory")
            else:
                print(f"Fail : Remove S3 {rm_ticker} directory")

    command = f"aws s3 cp {data_directory_path}/ s3://{bucket_name}/ --recursive --exclude '.DS_Store'"
    print(f"S3 Command : {command}")
    result = subprocess.run(command, shell=True, check=True, text=True)
    # 명령 실행 결과 확인
    if result.returncode == 0:
        print("Success : S3 upload")
    else:
        print("Fail : S3 upload")

    remove_files_in_directory(data_directory_path)