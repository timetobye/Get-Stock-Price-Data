import os
import pandas as pd
import time
import yfinance as yf

from utils.utility_functions import UtilityFunctions


def make_stock_dataframe(stock_ticker_list, **kwargs):
    est_data_interval_end = kwargs["data_interval_end"].in_timezone("America/New_York")
    start_date = est_data_interval_end.to_date_string()
    end_date = est_data_interval_end.add(days=1).to_date_string()

    dataframes = []  # 모든 종목 - 모든 기간
    mode = kwargs['dag_run'].conf.get('test_mode')
    if mode:
        stock_ticker_list = stock_ticker_list[0:10]

    for idx, ticker in enumerate(stock_ticker_list):
        try:
            stock_df = make_stock_history_dataframe(ticker, start_date, end_date)
            if stock_df is None:
                continue

            dataframes.append(stock_df)
            print(f"Success - idx : {idx}, ticker : {ticker}")
        except Exception as e:
            print(f"Error - idx : {idx}, ticker : {ticker}, e : {e}")
            stock_df = try_get_stock_dataframe(ticker, start_date, end_date)
            dataframes.append(stock_df)

        if (idx % 100) == 0:
            print(f"idx : {idx}, ticker : {ticker}")

    concat_df = pd.concat(dataframes, ignore_index=True)
    concat_df['date'] = pd.to_datetime(concat_df['date'])

    return concat_df


def make_stock_history_dataframe(ticker, start_date, end_date):
    upper_ticker = ticker.upper()
    stock = yf.Ticker(upper_ticker)

    # preprocessing part
    stock_history_df = stock.history(start=start_date, end=end_date, auto_adjust=False)
    if stock_history_df.empty:
        return None

    stock_history_df.reset_index(inplace=True)
    stock_history_df.columns = stock_history_df.columns.str.lower().str.replace(' ', '_')
    column_list = stock_history_df.columns.tolist()

    stock_history_df['date'] = stock_history_df['date'].dt.strftime('%Y-%m-%d')

    rounded_value_columns = ['open', 'high', 'low', 'close', 'adj_close']
    stock_history_df[rounded_value_columns] = stock_history_df[rounded_value_columns].round(2)

    # yfinance open source - key error - dividends
    if 'dividends' in column_list:
        stock_history_df['dividends'] = stock_history_df['dividends'].round(4)
    else:
        stock_history_df['Dividends'] = stock_history_df['Dividends'].round(4)
        stock_history_df.rename(columns={'Dividends': 'dividends'}, inplace=True)

    if 'capital_gains' in column_list:
        # 'capital_gains' 컬럼이 존재 하면 해당 컬럼 제거
        stock_history_df = stock_history_df.drop('capital_gains', axis=1)

    # Ticker 컬럼 추가
    stock_history_df['ticker'] = ticker

    # stock metadata
    stock_history_metadata = stock.history_metadata
    stock_type = stock_history_metadata.get('instrumentType')
    if stock_type is not None:
        stock_history_df['stock_type'] = stock_type.lower()
    else:
        stock_history_df['stock_type'] = None
        print(f"ticker : {upper_ticker}, Stock type is None")

    return stock_history_df


def try_get_stock_dataframe(ticker, start_date, end_date, max_retries=5, retry_interval=2.0):
    n = 0
    while n < max_retries:
        try:
            stock_df = make_stock_history_dataframe(ticker, start_date, end_date)
            return stock_df
        except Exception as e:
            print(f"Retry - ticker : {ticker}, e : {e}, n : {n}")
            n += 1
            time.sleep(retry_interval)

    raise ValueError(f"Failed to fetch data for ticker {ticker} after {max_retries} retries.")


def save_dataframe_to_csv(df, save_csv_file_dir_name):
    data_directory_path = UtilityFunctions.make_data_directory_path(save_csv_file_dir_name)

    date_str = df['date'].tolist()[0].strftime('%Y%m%d')
    directory_path = f"{data_directory_path}{os.sep}{date_str}"
    os.makedirs(directory_path, exist_ok=True)

    filename = f'{directory_path}{os.sep}market_{date_str}_{date_str}.csv'
    df.to_csv(filename, index=False)
