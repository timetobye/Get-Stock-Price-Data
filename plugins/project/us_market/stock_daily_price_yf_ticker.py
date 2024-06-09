import os
import pandas as pd
import time
import yfinance as yf
import io
import boto3


def upload_stock_data_to_s3(stock_ticker_list, **kwargs):
    est_data_interval_end = kwargs["data_interval_end"].in_timezone("America/New_York")
    start_date = est_data_interval_end.to_date_string()
    end_date = est_data_interval_end.add(days=1).to_date_string()

    mode = kwargs['dag_run'].conf.get('test_mode')
    if mode:
        stock_ticker_list = stock_ticker_list[0:10]
    
    for stock_ticker in stock_ticker_list:
        try:
            ticker_history_df = make_history_df(stock_ticker, start_date, end_date)
            if ticker_history_df is None:
                continue

        except Exception as e:
            ticker_history_df = retry_make_history_df(stock_ticker, start_date, end_date)

        upload_dataframe_to_s3(ticker_history_df, stock_ticker, start_date, test_mode=mode)


def upload_dataframe_to_s3(df, ticker, target_date, test_mode=False):
    date_str = target_date.replace('-', '')

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    from airflow.models import Variable
    aws_json = Variable.get(key="aws", deserialize_json=True)
    aws_access_key_id = aws_json["key"]["aws_access_key"]
    aws_secret_access_key = aws_json["key"]["aws_secret_access_key"]

    if test_mode:
        bucket_name = aws_json["aws_s3_bucket"]["test_bucket"]
    else:
        bucket_name = aws_json["aws_s3_bucket"]["yf_ticker_bucket"]

    file_name = f"{ticker}/{ticker}_{date_str}_{date_str}.csv"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())


def retry_make_history_df(ticker, start_date, end_date, max_retries=5, retry_interval=2.0):
    n = 0
    while n < max_retries:
        try:
            ticker_history_df = make_history_df(ticker, start_date, end_date)
            return ticker_history_df
        except Exception as e:
            print(f"Retry - ticker : {ticker}, e : {e}, n : {n}")
            n += 1
            time.sleep(retry_interval)

    raise ValueError(f"Failed to fetch data for ticker {ticker} after {max_retries} retries.")


def make_history_df(ticker, start_date, end_date):
    ticker = ticker.upper()
    stock = yf.Ticker(ticker)
    stock_history_metadata = stock.history_metadata
    stock_type = stock_history_metadata.get('instrumentType', None)
    if stock_type is None:
        print(f"ticker : {ticker}, Stock type is None")

    stock_history_df = stock.history(start=start_date, end=end_date, auto_adjust=False)
    if stock_history_df.empty:
        return None

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
