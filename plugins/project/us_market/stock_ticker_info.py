import pandas as pd
import time
import yfinance as yf

"""
Index에 속한 ticker 정보를 가져옵니다.
그리고 추가로 가져올 항목에 대한 목록을 정리 합니다.
최종적으로 index 정보와 ticker 목록을 반환 합니다.
"""


class StockDataHandler:
    def __init__(self):
        # TODO : Link 는 Airflow Variables 에 넣을지 확인 중. 오히려 코드 중복과 양이 늘어날 것 같음
        self.index_data_wiki_link_dict = {
            'S&P500': "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks",
            'NASDAQ100': "https://en.wikipedia.org/wiki/Nasdaq-100#Components",
            'DOW30': "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average#Components",
            'S&P400': "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies#S&P_400_MidCap_Index_Component_Stocks",
            'S&P600': "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies#S&P_600_component_stocks"
        }

    def _preprocess_dataframe(self, df):
        # AWS GLUE 에서 콤마 문제를 해결하기 위해 일부 컬럼의 경우 ' -' 처리
        df['GICS Sub-Industry'] = df['GICS Sub-Industry'].str.replace(',', ' -')

        # A class stock ticker(E.g. CWEN.A -> CWEN-A)
        df['Symbol'] = df['Symbol'].str.replace(r'\.A', '-A', regex=True)

        # B class stock ticker(E.g. BRK.B -> BRK-B)
        df['Symbol'] = df['Symbol'].str.replace(r'\.B', '-B', regex=True)

        # 작업 편의상 symbol 을 다시 ticker 로 변경
        df.rename(columns={'Symbol': 'Ticker'}, inplace=True)

        # 컬럼명을 소문자로 변경
        df.columns = df.columns.str.lower()

        return df

    def get_sp500_data(self):
        sp500_df = pd.read_html((self.index_data_wiki_link_dict.get('S&P500', None)), header=0)[0]

        sp500_column_selection = ['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']
        sp500_df = sp500_df[sp500_column_selection]

        sp500_column_rename = {'Security': 'Company'}
        sp500_df.rename(columns=sp500_column_rename, inplace=True)

        sp500_df = self._preprocess_dataframe(sp500_df)

        return sp500_df

    def get_nasdaq100_data(self):
        nasdaq100_df = pd.read_html((self.index_data_wiki_link_dict.get('NASDAQ100', None)), header=0)[4]

        nasdaq100_column_selection = ['Ticker', 'Company', 'GICS Sector', 'GICS Sub-Industry']
        nasdaq100_df = nasdaq100_df[nasdaq100_column_selection]

        nasdaq100_column_rename = {'Ticker': 'Symbol'}
        nasdaq100_df.rename(columns=nasdaq100_column_rename, inplace=True)

        nasdaq_100_df = self._preprocess_dataframe(nasdaq100_df)

        return nasdaq_100_df

    def get_dow30_data(self):
        dow30_df = pd.read_html((self.index_data_wiki_link_dict.get('DOW30', None)), header=0)[1]

        dow30_column_selection = ['Symbol', 'Company']
        dow30_df = dow30_df[dow30_column_selection]

        dow30_df['Symbol'] = dow30_df['Symbol'].str.replace(r'\.B', '-B', regex=True)
        dow30_df.rename(columns={'Symbol': 'Ticker'}, inplace=True)
        dow30_df.columns = dow30_df.columns.str.lower()

        return dow30_df

    def get_sp400_data(self):
        sp400_df = pd.read_html((self.index_data_wiki_link_dict.get('S&P400', None)), header=0)[0]

        sp400_column_selection = ['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']
        sp400_df = sp400_df[sp400_column_selection]

        sp400_column_rename = {'Security': 'Company'}
        sp400_df.rename(columns=sp400_column_rename, inplace=True)

        sp400_df = self._preprocess_dataframe(sp400_df)

        return sp400_df

    def get_sp600_data(self):
        sp600_df = pd.read_html((self.index_data_wiki_link_dict.get('S&P600', None)), header=0)[0]

        sp600_column_selection = ['Symbol', 'Company', 'GICS Sector', 'GICS Sub-Industry']
        sp600_df = sp600_df[sp600_column_selection]

        sp600_df = self._preprocess_dataframe(sp600_df)

        return sp600_df

    # TODO : 메소드 명 적절하게 다시 변경할 것
    def get_various_stock_data(self):
        from airflow.models import Variable
        variety_ticker_list = Variable.get(key="additional_ticker", deserialize_json=True)
        print(f"variety_ticker_list : {variety_ticker_list}")
        variety_df = pd.DataFrame(variety_ticker_list, columns=["ticker"])

        return variety_df

    # TODO: 이부분 수정 해야함 - 계속 수정 필요
    def combine_and_process_stock_data(self, dfs):
        # dfs = [sp500_df, nasdaq100_df, dow30_df, sp400_df, sp600_df, var_df]  # 20240106 기준
        sp500_df, nasdaq100_df, dow30_df = dfs[0], dfs[1], dfs[2]
        concat_df = pd.concat(dfs, ignore_index=True)

        # keep='first': 중복된 값 중 첫 번째로 나오는 레코드를 유지하고 나머지 중복 레코드는 제거
        concat_df.drop_duplicates(subset=['ticker'], keep='first', inplace=True)

        # 각 지수에 속한 종목을 1(True) 또는 0(False) 으로 표시하는 열 추가 - 대형주만 처리함
        concat_df['s&p500'] = concat_df['ticker'].isin(sp500_df['ticker']).astype(int)
        concat_df['nasdaq100'] = concat_df['ticker'].isin(nasdaq100_df['ticker']).astype(int)
        concat_df['dow30'] = concat_df['ticker'].isin(dow30_df['ticker']).astype(int)

        return concat_df

    # TODO : 향후에 데이터를 더 모아야 하는 코드로 변경 해야 하므로, 아래부터 끝까지 해당하는 코드는 별도 분리 할 예정
    def get_stock_df_with_info(self, stock_ticker_list):
        stock_info_list = []
        for ticker in stock_ticker_list:
            result = self._get_stock_info_with_retry(ticker)
            stock_info_list.append(result)

        stock_info_df = pd.DataFrame.from_dict(stock_info_list)  # Pandas 에서 만드는 방법

        return stock_info_df

    def _get_stock_info_with_retry(self, ticker):
        try:
            result = self._get_stock_info_data(ticker)

            return result

        except:
            n = 0
            max_rotate_value = 10
            while n < max_rotate_value:
                try:
                    result = self._get_stock_info_data(ticker)

                    return result

                except Exception as e:
                    n += 1
                    time.sleep(2.0)

                    if n >= max_rotate_value:
                        print(f"Fail - ticker : {ticker}, error : {e}, n : {n}")
                        raise ValueError

    def _get_stock_info_data(self, ticker):
        response_res = yf.Ticker(ticker)
        stock_yf_basic_info = response_res.info
        quote_type = stock_yf_basic_info['quoteType']

        if quote_type == 'EQUITY':
            marketCap_value = stock_yf_basic_info['marketCap']
            quote_type_lower = quote_type.lower()

            stock_simple_info_dict = {
                'ticker': ticker,
                'quote_type': quote_type_lower,
                'asset_size': marketCap_value,
            }

            return stock_simple_info_dict

        elif quote_type == 'ETF':
            quote_type_lower = quote_type.lower()

            stock_simple_info_dict = {
                'ticker': ticker,
                'quote_type': quote_type_lower,
                'asset_size': None
            }

            return stock_simple_info_dict

        elif quote_type == 'INDEX':
            quote_type_lower = quote_type.lower()

            stock_simple_info_dict = {
                'ticker': ticker,
                'quote_type': quote_type_lower,
                'asset_size': None
            }

            return stock_simple_info_dict
        else:
            print(f"Information is not exist in YF : {ticker}, "
                  f"stock_yf_basic_info['quoteType'] : {stock_yf_basic_info['quoteType']}")

            raise ValueError
