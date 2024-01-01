import empyrical as ep
import numpy as np
import os
import pandas as pd
import shutil
import yfinance as yf
import sys
sys.path.append('/opt/airflow/')

from datetime import datetime, timedelta
from utils.utility_functions import UtilityFunctions
from project.us_market.stock_ticker_info import GetTickerInfo

"""
1. 각 Ticker 별 상장 시작 -> 현재까지 데이터를 수집
2. S3 에는 Ticker 로 partition 을 설정하여 데이터 조회 할 수 있도록 데이터 업로드
3. 20240101 - 1월 내 추가 수정 예정
"""


class StockMDDProcessor:
    def __init__(self):
        pass

    def make_stock_mdd_csv_files(self):
        """
        yf 의 이슈로 API 호출이 정상적으로 진행이 되지 않는 경우가 종종 있어서 While 문으로 처리하였음
        """
        stock_index_wiki_df, stock_ticker_list = GetTickerInfo().get_ticker_info()
        print(stock_ticker_list[0:10])

        # stock_ticker_list = ['AAPL', 'SPY', '^GSPC']  # test

        for idx, ticker in enumerate(stock_ticker_list):
            stock_close_series_data = self._get_stock_close_series_data(ticker)
            maximum_drawdown_df = self._make_maximum_drawdown_df(stock_close_series_data)

            target_directory_path = self._make_stock_directory(ticker)  # 종목별로 디렉터리 만들기
            target_file_path = f"{target_directory_path}{os.sep}{ticker}_mdd.csv"
            maximum_drawdown_df.to_csv(target_file_path, index=False)

    def _get_stock_close_series_data(self, ticker):
        yf_ticker = yf.Ticker(ticker)
        yf_ticker_max_history = yf_ticker.history(period='max', auto_adjust=False)
        result_close_series_data = yf_ticker_max_history['Close'].pct_change()

        return result_close_series_data

    def _make_maximum_drawdown_df(self, data, top=300):
        mdd_period_df = self._show_worst_drawdown_periods(data, top=top)

        return mdd_period_df

    def _make_stock_directory(self, ticker):
        base_directory = UtilityFunctions.make_data_directory_path()
        stock_directory_path = f"{base_directory}{os.sep}{ticker}"
        os.makedirs(stock_directory_path, exist_ok=True)

        return stock_directory_path

    def _get_max_drawdown_underwater(self, underwater):
        """
        Determines peak, valley, and recovery dates given an 'underwater'
        DataFrame.
        An underwater DataFrame is a DataFrame that has precomputed
        rolling drawdown.
        Parameters
        ----------
        underwater : pd.Series
           Underwater returns (rolling drawdown) of a strategy.
        Returns
        -------
        peak : datetime
            The maximum drawdown's peak.
        valley : datetime
            The maximum drawdown's valley.
        recovery : datetime
            The maximum drawdown's recovery.
        """

        valley = underwater.idxmin()  # end of the period
        # Find first 0
        peak = underwater[:valley][underwater[:valley] == 0].index[-1]
        # Find last 0
        try:
            recovery = underwater[valley:][underwater[valley:] == 0].index[0]
        except IndexError:
            recovery = np.nan  # drawdown not recovered
        return peak, valley, recovery

    def _get_top_drawdowns(self, returns, top=10):
        """
        Finds top drawdowns, sorted by drawdown amount.
        Parameters
        ----------
        returns : pd.Series
            Daily returns of the strategy, noncumulative.
             - See full explanation in tears.create_full_tear_sheet.
        top : int, optional
            The amount of top drawdowns to find (default 10).
        Returns
        -------
        drawdowns : list
            List of drawdown peaks, valleys, and recoveries. See get_max_drawdown.
        """

        returns = returns.copy()
        df_cum = ep.cum_returns(returns, 1.0)
        running_max = np.maximum.accumulate(df_cum)
        underwater = df_cum / running_max - 1

        drawdowns = []
        for _ in range(top):
            peak, valley, recovery = self._get_max_drawdown_underwater(underwater)
            # Slice out draw-down period
            if not pd.isnull(recovery):
                underwater.drop(
                    underwater[peak:recovery].index[1:-1], inplace=True
                )
            else:
                # drawdown has not ended yet
                underwater = underwater.loc[:peak]

            drawdowns.append((peak, valley, recovery))
            if (
                    (len(returns) == 0)
                    or (len(underwater) == 0)
                    or (np.min(underwater) == 0)
            ):
                break

        return drawdowns


    def _gen_drawdown_table(self, returns, top=10):
        """
        Places top drawdowns in a table.
        Parameters
        ----------
        returns : pd.Series
            Daily returns of the strategy, noncumulative.
             - See full explanation in tears.create_full_tear_sheet.
        top : int, optional
            The amount of top drawdowns to find (default 10).
        Returns
        -------
        df_drawdowns : pd.DataFrame
            Information about top drawdowns.
        """

        df_cum = ep.cum_returns(returns, 1.0)
        drawdown_periods = self._get_top_drawdowns(returns, top=top)
        df_drawdowns = pd.DataFrame(
            index=list(range(top)),
            columns=[
                "Net drawdown in %",
                "Peak date",
                "Valley date",
                "Recovery date",
                "Duration",
            ],
        )

        for i, (peak, valley, recovery) in enumerate(drawdown_periods):
            if pd.isnull(recovery):
                df_drawdowns.loc[i, "Duration"] = np.nan
            else:
                df_drawdowns.loc[i, "Duration"] = len(
                    pd.date_range(peak, recovery, freq="B")
                )
            df_drawdowns.loc[i, "Peak date"] = peak.to_pydatetime().strftime("%Y-%m-%d")
            df_drawdowns.loc[i, "Valley date"] = valley.to_pydatetime().strftime("%Y-%m-%d")
            if isinstance(recovery, float):
                df_drawdowns.loc[i, "Recovery date"] = recovery
            else:
                df_drawdowns.loc[
                    i, "Recovery date"
                ] = recovery.to_pydatetime().strftime("%Y-%m-%d")
            # df_drawdowns.loc[i, "Net drawdown in %"] = ((df_cum.loc[peak] - df_cum.loc[valley]) / df_cum.loc[peak])
            df_drawdowns.loc[i, "Net drawdown in %"] = ((df_cum.loc[peak] - df_cum.loc[valley]) / df_cum.loc[peak]) * 100

        df_drawdowns["Peak date"] = pd.to_datetime(df_drawdowns["Peak date"])
        df_drawdowns["Valley date"] = pd.to_datetime(df_drawdowns["Valley date"])
        df_drawdowns["Recovery date"] = pd.to_datetime(
            df_drawdowns["Recovery date"]
        )

        return df_drawdowns


    def _show_worst_drawdown_periods(self, returns, top=5):
        """
        Prints information about the worst drawdown periods.
        Prints peak dates, valley dates, recovery dates, and net
        drawdowns.
        Parameters
        ----------
        returns : pd.Series
            Daily returns of the strategy, noncumulative.
             - See full explanation in tears.create_full_tear_sheet.
        top : int, optional
            Amount of top drawdowns periods to plot (default 5).
        """

        drawdown_df = self._gen_drawdown_table(returns, top=top)
        drawdown_df.sort_values("Net drawdown in %", ascending=False, inplace=True)
        drawdown_df.dropna(subset=['Net drawdown in %'], inplace=True)
        drawdown_df.reset_index(inplace=True)

        converted_column_list = {
            'index': 'worst_drawdown_periods',
            'Net drawdown in %': 'net_drawdown',
            'Peak date': 'peak_date',
            'Valley date': 'valley_date',
            'Recovery date': 'recovery_date',
            'Duration': 'duration'
        }
        drawdown_df.rename(columns=converted_column_list, inplace=True)

        return drawdown_df
