import pandas as pd
import sys
sys.path.append('/opt/airflow/')

"""
Index에 속한 ticker 정보를 가져옵니다.
그리고 추가로 가져올 항목에 대한 목록을 정리 합니다.
최종적으로 index 정보와 ticker 목록을 반환 합니다.
"""


class GetTickerInfo:
    def __init__(self):
        pass

    def get_ticker_info(self):
        stock_index_data_df = self._download_stock_index_data_from_wiki()

        stock_index_ticker_list = stock_index_data_df['ticker'].tolist()
        stock_ticker_list = self._make_stock_ticker_list(stock_index_ticker_list)

        return stock_index_data_df, stock_ticker_list

    def _download_stock_index_data_from_wiki(self):
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

    def _make_stock_ticker_list(self, basic_index_ticker_list):
        additional_symbol_list = [
            'DIA', 'SPY', 'QQQ', 'IWM', 'IWO', 'VTV',  # 6
            'XLK', 'XLY', 'XLV', 'XLF', 'XLI', 'XLP', 'XLU', 'XLB', 'XLE', 'XLC', 'XLRE',  # 11
            'COWZ', 'HYG', 'HYBB', 'STIP', 'SCHD', 'SPLG', 'IHI', 'TLT', 'KMLM', 'MOAT',  # 10
            'EWY', 'EWJ',  # 2
            'IYT',  # 1
            'BROS', 'SLG', 'EPR', 'ZIP', 'SMCI', 'PLTR', 'CPNG',  # 7
            '^GSPC', '^DJI', '^IXIC', '^RUT', '^TNX'
        ]

        add_result = basic_index_ticker_list + additional_symbol_list
        unique_ticker_list = list(dict.fromkeys(add_result).keys())  # Python 3.6+

        return unique_ticker_list
