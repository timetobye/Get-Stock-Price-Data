import pandas as pd


def get_stock_data_for_index():
    """
    wikipedia 데이터는 데이터 업데이트가 빠르고, 기록 추적에 편리하기 때문에 사용합니다.
    한국투자증권에서 제공하는 API 에서 각 종목이 특정 지수에 속하는지에 대한 자료가 정확하지 않아 해당 코드를 이용하여 데이터 추적
    """
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
    merged_df = pd.concat(dfs, ignore_index=True)

    # keep='first': 중복된 값 중 첫 번째로 나오는 레코드를 유지하고 나머지 중복 레코드는 제거
    merged_df.drop_duplicates(subset=['Symbol'], keep='first', inplace=True)

    # 각 지수에 속한 종목을 1(True) 또는 0(False) 으로 표시하는 열 추가
    merged_df['S&P500'] = merged_df['Symbol'].isin(s_and_p_500_df['Symbol']).astype(int)
    merged_df['NASDAQ100'] = merged_df['Symbol'].isin(nasdaq_100_df['Symbol']).astype(int)
    merged_df['DOW30'] = merged_df['Symbol'].isin(dow_30_df['Symbol']).astype(int)

    # merged_df.to_csv("result_df.csv", index=False)

    return merged_df


if __name__ == "__main__":
    get_stock_data_for_index()