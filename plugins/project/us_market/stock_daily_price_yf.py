import os
import pandas as pd
import time
import yfinance as yf

from datetime import datetime, timedelta
from utils.utility_functions import UtilityFunctions

# TODO : Class 이름 다시 명명 할 것 - 적잘한 이름으로!
class StockDataProcessor:
    def __init__(self):
        pass

    def generate_stock_dataframe(self, target_date, stock_ticker_list):
        """
        yf 의 이슈로 API 호출이 정상적으로 진행이 되지 않는 경우가 종종 있어서 While 문으로 처리하였음
        """
        # TODO : change position

        print(stock_ticker_list[0:10])
        dataframes = []  # 모든 종목 - 모든 기간
        # stock_ticker_list = ["SPY", "QQQ", "AAPL"]  # test
        for idx, ticker in enumerate(stock_ticker_list):
            try:
                stock_df = self._get_stock_dataframe(ticker, target_date)
                dataframes.append(stock_df)
            except Exception as e:
                print(f"idx : {idx}, ticker : {ticker}, error : {e}")

                n = 0
                max_rotate_value = 10
                while n < max_rotate_value:
                    try:
                        stock_df = self._get_stock_dataframe(ticker, target_date)
                        dataframes.append(stock_df)
                        print(f"Success - idx : {idx}, ticker : {ticker}, n : {n}")
                        break

                    except Exception as e:
                        print(f"Error - idx : {idx}, ticker : {ticker}, e : {e}, n : {n}")
                        n += 1
                        time.sleep(2.0)

                        if n >= max_rotate_value:
                            print(f"Fail - idx : {idx}, ticker : {ticker}, error : {e}, n : {n}")
                            raise ValueError

            if (idx % 100) == 0:
                print(f"idx : {idx}, ticker : {ticker}")

        # 데이터프레임 하나로 합치기
        concat_df = pd.concat(dataframes, ignore_index=True)
        concat_df['date'] = pd.to_datetime(concat_df['date'])

        return concat_df

    def save_dataframe_to_csv(self, df, save_csv_file_dir_name):
        data_directory_path = UtilityFunctions.make_data_directory_path(save_csv_file_dir_name)

        group_by_concat_df = df.groupby(df['date'])
        for idx, (date, group) in enumerate(group_by_concat_df):
            date_str = date.strftime("%Y%m%d")
            directory_path = f"{data_directory_path}{os.sep}{date_str}"
            os.makedirs(directory_path, exist_ok=True)

            filename = f'{directory_path}{os.sep}market_{date_str}_{date_str}.csv'
            group.to_csv(filename, index=False)

    def _make_history_data(self, ticker, start_date, end_date):
        ticker = ticker.upper()
        stock = yf.Ticker(ticker)

        if start_date is None and end_date is None:
            stock_history_df = stock.history(start='1993-01-01', auto_adjust=False)
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

    def _get_stock_dataframe(self, ticker, target_date):
        # target_date : YYYYMMDD
        date_obj = datetime.strptime(target_date, "%Y%m%d")
        next_day = date_obj + timedelta(days=1)

        start_date = date_obj.strftime("%Y-%m-%d")
        end_date = next_day.strftime("%Y-%m-%d")

        # print(f"start_date, end_date : {start_date}, {end_date}")

        ticker_history_df = self._make_history_data(ticker, start_date, end_date)

        return ticker_history_df
