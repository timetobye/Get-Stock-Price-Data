import time

import pandas as pd
import yfinance as yf


class IndexDataHandler:
    """
    Index에 속한 ticker 정보를 가져옵니다.
    그리고 추가로 가져올 항목에 대한 목록을 정리 합니다.
    최종적으로 index 정보와 ticker 목록을 반환 합니다.
    """

    # 2025-01-30 기준, WIKI 문서 수정에 따라 변경 될 수 있음
    TABLE_INDEX = {"S&P500": 0, "NASDAQ100": 4, "DOW30": 2, "S&P400": 0, "S&P600": 0}

    INDEX_WIKI_LINK = {
        "S&P500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks",
        "NASDAQ100": "https://en.wikipedia.org/wiki/Nasdaq-100#Components",
        "DOW30": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average#Components",
        "S&P400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies#S&P_400_MidCap_Index_Component_Stocks",
        "S&P600": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies#S&P_600_component_stocks",
    }

    def __init__(self):
        # self.index_name = index_name
        pass

    def __str__(self):
        return "인덱스에 해당하는 티커 정보들을 등록합니다"

    # TODO : 메소드 이름 정리 할 것
    def fetch_index_components(self, index_name):
        data = {
            "S&P500": self._get_sp500_data(),
            "NASDAQ100": self._get_nasdaq100_data(),
            "DOW30": self._get_dow30_data(),
            "S&P400": self._get_sp400_data(),
            "S&P600": self._get_sp600_data(),
        }
        return data[index_name]

    def _preprocess_dataframe(self, df):
        """
        공통 전처리 작업 수행
        'Symbol' 을 'Ticker' 로 변경하고, 기타 규칙에 맞게 수정
        """

        # AWS GLUE 에서 콤마 문제를 해결하기 위해 일부 컬럼의 경우 ' -' 처리
        df["GICS Sub-Industry"] = df["GICS Sub-Industry"].str.replace(",", " -")

        # A, B class stock ticker(E.g. CWEN.A -> CWEN-A, BRK.B -> BRK-B)
        df["Symbol"] = df["Symbol"].str.replace(r"\.A", "-A", regex=True)
        df["Symbol"] = df["Symbol"].str.replace(r"\.B", "-B", regex=True)

        # 작업 편의상 symbol 을 다시 ticker 로 변경
        df.rename(
            columns={
                "Symbol": "Ticker",
                "GICS Sector": "sector",
                "GICS Sub-Industry": "sub_sector",
            },
            inplace=True,
        )

        # 컬럼명을 소문자로 변경
        df.columns = df.columns.str.lower()

        return df

    def _get_data(
        self,
        index_name: str,
        column_selection: list,
        column_rename: dict = None,
        add_columns: list = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        공통된 데이터 가져오기 메서드
        """
        wiki_link = self.INDEX_WIKI_LINK.get(index_name)
        if wiki_link is None:
            raise ValueError(f"Data for {index_name} not found in wikipedia")

        table_index = self.TABLE_INDEX.get(index_name)
        if table_index is None:
            raise ValueError(f"Table index for {index_name} is not defined.")

        # 표를 가져온 후 원하는 컬럼 선택
        try:
            df = pd.read_html(wiki_link, header=0)[table_index]
            df = df[column_selection]
        except Exception as e:
            raise ValueError(f"Error fetching data from {wiki_link}: {e}")

        # 컬럼 이름 변경
        if column_rename:
            df.rename(columns=column_rename, inplace=True)

        if add_columns:
            for col in add_columns:
                df[col] = ""  # 빈 값으로 추가

        df["index_type"] = index_name

        df = self._preprocess_dataframe(df)

        return df

    def _get_sp500_data(self):
        column_selection = ["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]
        column_rename = {"Security": "Company"}

        return self._get_data("S&P500", column_selection, column_rename)

    def _get_nasdaq100_data(self):
        # column_selection = ["Symbol", "Company", "GICS Sector", "GICS Sub-Industry"]
        column_selection = ["Ticker", "Company", "GICS Sector", "GICS Sub-Industry"]
        column_rename = {"Ticker": "Symbol"}

        return self._get_data("NASDAQ100", column_selection, column_rename)

    def _get_dow30_data(self):
        column_selection = ["Symbol", "Company"]
        column_rename = None
        add_columns = ["GICS Sector", "GICS Sub-Industry"]

        return self._get_data(
            "DOW30", column_selection, column_rename, add_columns=add_columns
        )

    def _get_sp400_data(self):
        column_selection = ["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]
        column_rename = {"Security": "Company"}

        return self._get_data("S&P400", column_selection, column_rename)

    def _get_sp600_data(self):
        column_selection = ["Symbol", "Company", "GICS Sector", "GICS Sub-Industry"]

        return self._get_data("S&P600", column_selection)
