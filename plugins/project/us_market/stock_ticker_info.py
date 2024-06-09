import pandas as pd
import time
import yfinance as yf


class StockDataHandler:
    """
    Index에 속한 ticker 정보를 가져옵니다.
    그리고 추가로 가져올 항목에 대한 목록을 정리 합니다.
    최종적으로 index 정보와 ticker 목록을 반환 합니다.
    """
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
        sp_500_wiki = self.index_data_wiki_link_dict.get('S&P500', None)
        sp500_df = pd.read_html(sp_500_wiki, header=0)[0]

        sp500_column_selection = ['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']
        sp500_df = sp500_df[sp500_column_selection]

        sp500_column_rename = {'Security': 'Company'}
        sp500_df.rename(columns=sp500_column_rename, inplace=True)

        sp500_df = self._preprocess_dataframe(sp500_df)

        return sp500_df

    def get_nasdaq100_data(self):
        nasdaq100_wiki = self.index_data_wiki_link_dict.get('NASDAQ100', None)
        nasdaq100_df = pd.read_html(nasdaq100_wiki, header=0)[4]

        nasdaq100_column_selection = ['Ticker', 'Company', 'GICS Sector', 'GICS Sub-Industry']
        nasdaq100_df = nasdaq100_df[nasdaq100_column_selection]

        nasdaq100_column_rename = {'Ticker': 'Symbol'}
        nasdaq100_df.rename(columns=nasdaq100_column_rename, inplace=True)

        nasdaq_100_df = self._preprocess_dataframe(nasdaq100_df)

        return nasdaq_100_df

    def get_dow30_data(self):
        dow30_wiki = self.index_data_wiki_link_dict.get('DOW30', None)
        dow30_df = pd.read_html(dow30_wiki, header=0)[1]

        dow30_column_selection = ['Symbol', 'Company']
        dow30_df = dow30_df[dow30_column_selection]

        dow30_df['Symbol'] = dow30_df['Symbol'].str.replace(r'\.B', '-B', regex=True)
        dow30_df.rename(columns={'Symbol': 'Ticker'}, inplace=True)
        dow30_df.columns = dow30_df.columns.str.lower()

        return dow30_df

    def get_sp400_data(self):
        sp400_wiki = self.index_data_wiki_link_dict.get('S&P400', None)
        sp400_df = pd.read_html(sp400_wiki, header=0)[0]

        sp400_column_selection = ['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']
        sp400_df = sp400_df[sp400_column_selection]

        sp400_column_rename = {'Security': 'Company'}
        sp400_df.rename(columns=sp400_column_rename, inplace=True)

        sp400_df = self._preprocess_dataframe(sp400_df)

        return sp400_df

    def get_sp600_data(self):
        sp600_wiki = self.index_data_wiki_link_dict.get('S&P600', None)
        sp600_df = pd.read_html(sp600_wiki, header=0)[0]

        sp600_column_selection = ['Symbol', 'Company', 'GICS Sector', 'GICS Sub-Industry']
        sp600_df = sp600_df[sp600_column_selection]

        sp600_df = self._preprocess_dataframe(sp600_df)

        return sp600_df

    def get_various_stock_data(self):
        from airflow.models import Variable
        variety_ticker_list = Variable.get(key="additional_ticker", deserialize_json=True)
        print(f"variety_ticker_list : {variety_ticker_list}")
        various_stock_df = pd.DataFrame(variety_ticker_list, columns=["ticker"])

        return various_stock_df

    def combine_and_process_stock_data(self, dfs):
        """
        :param dfs: [sp500_df, nasdaq100_df, dow30_df, sp400_df, sp600_df, var_df]  # 20240106 기준
        :return: concat_df from dfs
        """
        concat_df = pd.concat(dfs, ignore_index=True)

        # keep='first': 중복된 값 중 첫 번째로 나오는 레코드를 유지하고 나머지 중복 레코드는 제거
        concat_df.drop_duplicates(subset=['ticker'], keep='first', inplace=True)

        index_dfs = {
            's&p500': dfs[0],
            'nasdaq100': dfs[1],
            'dow30': dfs[2]
        }

        # 각 지수에 속한 종목을 1(True) 또는 0(False) 으로 표시하는 열 추가 - 대형주만 처리함
        for index_name, index_df in index_dfs.items():
            concat_df[index_name] = concat_df['ticker'].isin(index_df['ticker']).astype(int)

        return concat_df

    def make_stock_info_df(self, stock_ticker_list, **kwargs):
        mode = kwargs['dag_run'].conf.get('test_mode')
        if mode:
            stock_ticker_list = stock_ticker_list[0:10]

        stock_info_list = []
        for ticker in stock_ticker_list:
            result = self._get_stock_info(ticker)
            stock_info_list.append(result)

        # Pandas 에서 데이터프레임 만드는 방법
        stock_info_df = pd.DataFrame.from_dict(stock_info_list)

        return stock_info_df

    def _get_stock_info(self, ticker):
        try:
            stock_info = self._get_stock_info_data(ticker)
            return stock_info
        except:
            stock_info = self._try_get_stock_info(ticker)
            return stock_info

    def _try_get_stock_info(self, ticker, max_retries=5, retry_interval=2.0):
        n = 0
        while n < max_retries:
            try:
                stock_info = self._get_stock_info_data(ticker)
                return stock_info
            except Exception as e:
                print(f"Retry - ticker : {ticker}, e : {e}, n : {n}")
                n += 1
                time.sleep(retry_interval)

        raise ValueError(f"Failed to fetch stock info for ticker: {ticker}")

    def _get_stock_info_data(self, ticker):
        yf_ticker_obj = yf.Ticker(ticker)
        stock_info = yf_ticker_obj.info
        quote_type = stock_info.get('quoteType')

        if quote_type == 'EQUITY':
            stock_simple_info_dict = {
                'ticker': ticker,
                'quote_type': quote_type.lower(),
                'asset_size': stock_info.get('marketCap'),
            }

            return stock_simple_info_dict

        elif quote_type == 'ETF':
            stock_simple_info_dict = {
                'ticker': ticker,
                'quote_type': quote_type.lower(),
                'asset_size': stock_info.get('totalAssets')
            }

            return stock_simple_info_dict

        elif quote_type == 'INDEX':
            stock_simple_info_dict = {
                'ticker': ticker,
                'quote_type': quote_type.lower(),
                'asset_size': None
            }

            return stock_simple_info_dict

        elif quote_type is None:
            stock_simple_info_dict = {
                'ticker': None,
                'quote_type': None,
                'asset_size': None
            }

            return stock_simple_info_dict

        else:
            print(f"Information is not exist in YF : {ticker}")

            raise ValueError
