import os
import pandas as pd
import time
import yfinance as yf
import pendulum

from utils.utility_functions import UtilityFunctions


# TODO : Class 이름 다시 명명 할 것 - 적잘한 이름으로!
class StockDataProcessor:
    def __init__(self):
        pass

    def generate_stock_dataframe(self, target_date, stock_ticker_list):
        """
        yf 의 이슈로 API 호출이 정상적으로 진행이 되지 않는 경우가 종종 있어서 While 문으로 처리하였음
        """
        # TODO : 이 부분을 어떻게 해서든 깔끔하게 해결해야 함
        dataframes = []  # 모든 종목 - 모든 기간

        for idx, ticker in enumerate(stock_ticker_list):
            try:
                stock_df = self._get_stock_dataframe(ticker, target_date)
                dataframes.append(stock_df)

            except Exception as error:
                n, max_rotate_value = 0, 5
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

                print(f"Fail - idx : {idx}, ticker : {ticker}, error : {error}, n : {n}")
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
    
    def _get_stock_dataframe(self, ticker, target_date):
        parse_date = pendulum.parse(target_date)
        start_date = parse_date.to_date_string()
        end_date = parse_date.add(days=1).to_date_string()

        try:
            ticker_history_df = self._make_stock_history_dataframe(ticker, start_date, end_date)

            return ticker_history_df

        # TODO : 에러 처리 코드 수정이 필요
        except Exception as e:
            raise Exception(f"Exception Error")

    def _make_stock_history_dataframe(self, ticker, start_date, end_date):
        upper_ticker = ticker.upper()
        stock = yf.Ticker(upper_ticker)

        # preprocessing part
        stock_history_df = stock.history(start=start_date, end=end_date, auto_adjust=False)
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
            # 'capital_gains' 컬럼이 존재하면 해당 컬럼 제거
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



