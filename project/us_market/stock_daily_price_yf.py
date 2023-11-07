import os
import pandas as pd
import shutil
import yfinance as yf
import sys
sys.path.append('/opt/airflow/')

from datetime import datetime, timedelta
from project.us_market.stock_ticker_info import GetTickerInfo
from utils.configuration_use import ConfigurationUse


class StockDataProcessor(ConfigurationUse):
    def __init__(self):
        super().__init__()

    def get_data_directory_path(self):
        target_directory_name = "airflow/data"
        # current_directory_path = os.getcwd()
        parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
        target_directory_path = os.path.join(parent_directory_path, target_directory_name)

        return target_directory_path

    def remove_files_in_directory(self, directory_path):
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)

            if os.path.isfile(item_path):
                # 파일일 경우 제거
                os.remove(item_path)
            elif os.path.isdir(item_path):
                # 디렉터리일 경우 shutil.rmtree()를 사용하여 재귀적으로 제거
                shutil.rmtree(item_path)

    def get_stock_df_csv_files(self, target_date):
        """
        yf 의 이슈로 API 호출이 정상적으로 진행이 되지 않는 경우가 종종 있어서 While 문으로 처리하였음
        """
        stock_index_wiki_df, stock_ticker_list = GetTickerInfo().get_ticker_info()
        data_directory_path = self.get_data_directory_path()

        print(stock_ticker_list[0:10])
        dataframes = []  # 모든 종목 - 모든 기간
        # stock_ticker_list = ["SPY", "QQQ" ,"AAPL"]  # test
        for idx, ticker in enumerate(stock_ticker_list):
            try:
                stock_df = self._get_stock_dataframe(ticker, target_date, stock_index_wiki_df)
                dataframes.append(stock_df)
            except Exception as e:
                print(f"idx : {idx}, ticker : {ticker}, error : {e}")

                n = 0
                while n < 5:
                    try:
                        stock_df = self._get_stock_dataframe(ticker, target_date, stock_index_wiki_df)
                        dataframes.append(stock_df)
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

        # 데이터프레임 하나로 합치기
        concat_df = pd.concat(dataframes, ignore_index=True)
        concat_df['date'] = pd.to_datetime(concat_df['date'])

        for idx, (date, group) in enumerate(concat_df.groupby(concat_df['date'])):
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

    def _get_stock_dataframe(self, ticker, target_date, wiki_df):
        formatted_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
        start_date = formatted_date
        date_obj = datetime.strptime(formatted_date, "%Y-%m-%d")
        next_day = date_obj + timedelta(days=1)
        end_date = next_day.strftime("%Y-%m-%d")
        print(f"start_date, end_date : {start_date}, {end_date}")

        ticker_history_df = self._make_history_data(ticker, start_date, end_date)
        ticker_history_df = ticker_history_df.merge(
            wiki_df[['ticker', 's&p500', 'nasdaq100', 'dow30']],
            on='ticker',
            how='left'
        )

        return ticker_history_df

    def get_slack_channel_info(self):
        slack_message_token = self.config_object.get('SLACK_CONFIG', 'channel_access_token')
        channel_name = self.config_object.get('SLACK_CONFIG', 'channel_name')
        slack_channel_name = f"#{channel_name}"

        slack_channel_info = {
            'channel': slack_channel_name,
            'token': slack_message_token
        }

        return slack_channel_info