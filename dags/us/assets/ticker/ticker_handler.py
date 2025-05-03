import time
from pprint import pprint

import pandas as pd
import yfinance as yf


def download_tickers_price(idx, **kwargs):
    start_date = (
        kwargs["data_interval_start"].in_timezone("Asia/Seoul").to_date_string()
    )
    end_date = kwargs["data_interval_end"].in_timezone("Asia/Seoul").to_date_string()

    print(f"start_date : {start_date}, end_date : {end_date}")

    ti = kwargs["ti"]
    ticker_list = ti.xcom_pull(
        task_ids="get_ticker_list_from_aws_task", key="ticker_list"
    )

    batch_size = 50
    start_idx = idx * batch_size
    end_idx = start_idx + batch_size
    batch_tickers = ticker_list[start_idx:end_idx]
    upper_tickers = [ticker.upper() for ticker in batch_tickers]

    yf_ticker_df_list = []
    yf_ticker_error_list = []
    for index, ticker in enumerate(upper_tickers):
        try:
            yf_ticker = yf.Ticker(ticker)

            yf_ticker_df = yf_ticker.history(
                start=start_date, end=end_date, auto_adjust=False
            )
            # Daily Price only
            yf_ticker_df["ticker"] = ticker
            if yf_ticker_df.empty:
                continue

            yf_ticker_df_list.append(yf_ticker_df.reset_index())

            if (index % 5) == 0:
                time.sleep(1.5)
        except Exception as e:
            print(f"Error : {e}, Ticker : {ticker}")
            yf_ticker_error_list.append(ticker)

    print(f"yf_ticker_error_list : {yf_ticker_error_list}")
    yf_download_df = pd.concat(yf_ticker_df_list, ignore_index=True)

    """
    yf.download 기능이 정상적이지 않아 다른 코드로 대체
    yf_download_df = yf.download(
        tickers=upper_tickers,
        start=start_date,
        end=end_date,
        # period="1y",
        group_by="Ticker",
        auto_adjust=False,
        actions=True,
    )  # stack level=0 후 reset_index 이용하여 멀티인덱스 / 컬럼 정리

    yf_download_df = yf_download_df.stack(level=0).reset_index()
    """

    # preprocessing part
    yf_download_df.columns = yf_download_df.columns.str.lower()
    yf_download_df.columns = yf_download_df.columns.str.replace(" ", "_")

    round_columns = ["open", "high", "low", "close", "adj_close"]
    yf_download_df[round_columns] = yf_download_df[round_columns].round(3)
    yf_download_df["dividends"] = yf_download_df["dividends"].round(4)

    # 'date' 열을 datetime으로 변환하면서 UTC로 변환
    yf_download_df["date"] = pd.to_datetime(
        yf_download_df["date"], format="mixed", utc=True
    )

    # UTC 시간을 뉴욕 시간(EST/EDT)으로 변환
    yf_download_df["date"] = yf_download_df["date"].dt.tz_convert("America/New_York")
    yf_download_df["date"] = yf_download_df["date"].dt.strftime("%Y-%m-%d")

    if "capital_gains" in yf_download_df.columns:
        # 'capital_gains' 컬럼이 존재하면 해당 컬럼 제거
        yf_download_df.drop("capital_gains", axis=1, inplace=True)

    pprint(yf_download_df)

    ti.xcom_push(key=f"batch_{idx}", value=yf_download_df.to_json())  # JSON 형태로 저장


def merge_ticker_batch_dataframes(**kwargs):
    """모든 batch 데이터를 합치는 함수"""
    ti = kwargs["ti"]

    # XCom에서 모든 batch 데이터 가져오기
    batch_dataframes = []

    # TODO : 병렬 실행된 batch 개수를 자동으로 가져와서 입력하기, 현재는 하드코딩
    for i in range(20):  # 병렬 실행된 batch 개수
        json_data = ti.xcom_pull(task_ids=f"fetch_ticker_price_tasks", key=f"batch_{i}")
        if json_data:
            df = pd.read_json(json_data[0])
            batch_dataframes.append(df)

    # 데이터프레임 병합
    merged_df = pd.concat(batch_dataframes, ignore_index=True)

    # 최종 결과를 다시 XCom에 저장 (필요하다면 S3 업로드도 가능)
    ti.xcom_push(key="merged_ticker_price_data", value=merged_df)
