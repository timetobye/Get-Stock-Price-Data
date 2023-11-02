import argparse
import calendar
import configparser
import glob
import json
import os
import pandas as pd
import random
import requests
import time
import exchange_calendars as xcals

from datetime import date, datetime, timedelta


def get_config_object():
    config_ini_file_name = "config.ini"
    parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
    config_ini_file_path = f"{parent_directory_path}{os.sep}config{os.sep}{config_ini_file_name}"

    config_object = configparser.ConfigParser()
    config_object.read(config_ini_file_path)

    return config_object


def get_config():
    config_object = get_config_object()
    app_key = config_object.get('LIVE_APP', 'KEY')
    app_secret_key = config_object.get('LIVE_APP', 'SECRET_KEY')
    app_token = config_object.get('LIVE_APP', 'ACCESS_TOKEN')

    return app_key, app_secret_key, app_token


def get_us_market_status(target_date):
    xnys = xcals.get_calendar("XNYS")
    open_status = xnys.is_session(target_date)

    return open_status


def preprocessing_csv_to_dict(file_path):
    origin_code_file_df = pd.read_csv(file_path, dtype=str)
    result_dict = origin_code_file_df.groupby('Symbol').apply(
        lambda x: x.drop('Symbol', axis=1).to_dict('records')
    ).to_dict()

    # print(f"pre part result_dict : {result_dict}")

    return result_dict


def read_stock_code_data(target_directory_path):
    target_stock_code_dict = {}
    print(f" self.target_directory_path : {target_directory_path} !!!!@@#@%%#^$#%^@#$%#@$5")
    # code_csv_file_list = os.listdir(self.target_directory_path)
    code_csv_file_list = glob.glob(f"{target_directory_path}{os.sep}*.csv")
    delimiter = os.sep
    # print(f"#@$#%$#%#@^ code_csv_file_path_list : {code_csv_file_list}")

    for idx, file_path in enumerate(code_csv_file_list):
        # print(f"idx idx idx 8***** : {idx}")
        # file_path = os.path.join(self.target_directory_path, file_name)

        result = preprocessing_csv_to_dict(file_path)

        # print(f"read part result_dict : {result}")

        target_stock_code_dict.update(result)

        # print(f"target_stock_code_dict : {target_stock_code_dict} **&^%&*(*&^%$%^&")

        os.remove(file_path)

    # print(f"target_stock_code_dict : {target_stock_code_dict}")

    return target_stock_code_dict


def get_stock_data(exchange_code, ticker, target_date):
    """
    거래소코드
    - NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스
    - HKS: 홍콩, TSE: 도쿄, SHS: 상해, SZS: 심천, SHI: 상해지수, SZI: 심천지수
    - HSX: 호치민, HNX: 하노이
    - BAY: 뉴욕(주간), BAQ: 나스닥(주간), BAA: 아멕스(주간)
    """
    key, secret_key, token = get_config()
    url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
    path = "/uapi/overseas-price/v1/quotations/dailyprice"
    url = f"{url_base}/{path}"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appKey": key,
        "appSecret": secret_key,
        "tr_id": "HHDFS76240000"
    }

    params = {
        "AUTH": "",
        "EXCD": exchange_code,  # 거래소코드
        "SYMB": ticker,  # 종목코드 (ex. TSLA)
        "GUBN": "0",  # 0: 일, 1: 주, 2: 월
        "BYMD": target_date,  # 조회 날짜 : YYYYMMDD
        "MODP": "1",  # 수정주가반영여부, 0: 미반영, 1: 반영
        "fid_org_adj_prc": "0",
    }

    res = requests.get(url, headers=headers, params=params)

    return res.json()


def convert_daily_data(data_result):
    return pd.DataFrame([data_result])


def convert_json_to_csv(symbol, stock_json, stock_info):
    stock_rsym = stock_json['output1']['rsym']
    stock_ticker = stock_rsym[4:]  # "D+시장구분(3자리)+종목코드 : DNASAAPL : D+NAS(나스닥)+AAPL(애플)"
    stock_exchange_code = stock_rsym[1:4]

    output_one = stock_json['output1']  # output1 전체
    output_two = stock_json['output2'][0]  # output2 - 1일 데이터(원래는 100일 데이터 한 번에 리턴)

    output_one_df = convert_daily_data(output_one)
    output_two_df = convert_daily_data(output_two)

    daily_stock_data_df = pd.concat([output_two_df, output_one_df], axis=1)
    daily_stock_data_df['xymd'] = daily_stock_data_df['xymd'].apply(
        lambda x: pd.to_datetime(str(x), format='%Y%m%d')
    )

    daily_stock_data_df['symbol'] = symbol
    daily_stock_data_df['DOW30'] = str(stock_info[0]['DOW30'])
    daily_stock_data_df['NAS100'] = str(stock_info[0]['NASDAQ100'])
    daily_stock_data_df['S&P500'] = str(stock_info[0]['S&P500'])
    daily_stock_data_df['korea_name'] = stock_info[0]['korea_name']
    daily_stock_data_df['english_name'] = stock_info[0]['english_name']
    daily_stock_data_df['security_type'] = stock_info[0]['security_type']
    daily_stock_data_df['exchange_code'] = stock_info[0]['exchange_code']
    daily_stock_data_df['exchange_name'] = stock_info[0]['exchange_name']
    daily_stock_data_df['GICS Sector'] = stock_info[0]['GICS Sector']
    daily_stock_data_df['GICS Sub-Industry'] = stock_info[0]['GICS Sub-Industry']
    daily_stock_data_df['Company'] = stock_info[0]['Company']

    return daily_stock_data_df


def concat_and_save_stock_dataframe(target_directory_path, stock_info_dict, target_date):
    error_count = 0
    error_ticker_list = []
    dfs = []

    for idx, (symbol, stock_info) in enumerate(stock_info_dict.items()):
        # print(f"Download {symbol} - data - {stock_info}, Date : {target_date}")
        stock_ticker = symbol
        stock_exchange_code = stock_info[0]['exchange_code']
        stock_json = get_stock_data(stock_exchange_code, stock_ticker, target_date)

        try:
            result_df = convert_json_to_csv(symbol, stock_json, stock_info)
            dfs.append(result_df)

        except Exception as e:
            print(f"error : {e}, symbol : {symbol}, target_date : {target_date}")
            error_count += 1
            error_ticker_list.append(symbol)

        # print("--------------------------------------------------")
        time.sleep(0.1)

        if (idx % 200) == 0:
            print(f"idx : {idx}")
            time.sleep(0.2)

    print(f"error count : {error_count}, error_ticker_list : {error_ticker_list}, target_date :{target_date}")
    daily_stock_data_df = pd.concat(dfs, ignore_index=True)
    daily_stock_data_df.rename(columns={'clos':'close'}, inplace=True)

    csv_file_name = f"{target_directory_path}{os.sep}" \
                    f"us_market_{target_date}_{target_date}.csv"
    daily_stock_data_df.to_csv(csv_file_name, index=False)


def make_target_year_month_list(start_year_month_str, end_year_month_str):
    # 입력 받은 두 개의 yyyymm 값을 datetime 객체로 변환
    start_date = datetime.strptime(start_year_month_str, "%Y%m")  # 202303 -> 2023-03-01 00:00:00
    end_date = datetime.strptime(end_year_month_str, "%Y%m")

    # start_date부터 end_date까지의 월별 값을 생성
    result = []
    while start_date <= end_date:
        result.append(start_date.strftime("%Y%m"))
        start_date = start_date + timedelta(days=32)
        start_date = start_date.replace(day=1)  # day 부분을 1로 변경

    return result


def get_target_date_in_months(target_year_month_str):
    today = datetime.date(datetime.today())
    year, month = int(target_year_month_str[:4]), int(target_year_month_str[4:])

    first_day = date(year, month, 1)  # 월의 첫 날
    last_day = date(year, month, calendar.monthrange(year, month)[1])  # 월의 마지막 날짜

    result = []
    current_day = first_day
    while current_day <= last_day:
        # 현재 날짜가 주말인지 확인(0: 월요일, 6: 일요일)
        if current_day.weekday() <= 4:
            # 현재 날짜가 조회 날짜보다 이전인 경우에만 결과에 추가
            if current_day <= today:
                result.append(current_day.strftime("%Y%m%d"))
        current_day += timedelta(days=1)

    return result


def get_data_directory_path():
    target_directory_name = "data"
    parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
    target_directory_path = os.path.join(parent_directory_path, target_directory_name)

    return target_directory_path


# '-l','--list', nargs='+'
if __name__ == "__main__":
    """
    # 일정 기간으로 지정해서 다운로드 하는 경우
    python3 backfill_us_market_stock_daily_price_month.py --start_year_and_month 202309 --end_year_and_month 202309

    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--start_year_and_month",
        type=str,
        default=None,
        help="Input Year&Month",
        required=True
    )

    arg_parser.add_argument(
        "--end_year_and_month",
        type=str,
        default=None,
        help="Input Year&Month",
        required=True
    )

    args = arg_parser.parse_args()
    target_start_year_and_month = args.start_year_and_month
    target_end_year_and_month = args.end_year_and_month

    if target_start_year_and_month is None or target_end_year_and_month is None:
        print(f"Please Input : target_start_year_and_month and target_end_year_and_month")
        raise ValueError

    market_data_path = get_data_directory_path()
    print(f"market_data_path : {market_data_path}")
    stock_code_dict_data = read_stock_code_data(market_data_path)

    year_month_list = make_target_year_month_list(target_start_year_and_month, target_end_year_and_month)
    print(year_month_list)
    for idx, year_month in enumerate(year_month_list):
        target_date_list = get_target_date_in_months(year_month)
        print(target_date_list)

        # target_date_list = ['20230901', '20230904', '20230905']  # test : 20230904 - 미장 휴장
        # target_date_list = ['20230901']  # test : 20230904 - 미장 휴장
        # target_date_list = ['20230801']  # test : 20230904 - 미장 휴장

        delay_seconds = random.randint(5, 6)

        print(f"Waiting for {delay_seconds} seconds...")
        time.sleep(delay_seconds)  # 선택된 초만큼 대기

        print(f"{year_month} Start!")
        # raise Exception
        # 하루 단위로 데이터프레임을 만듭니다.
        for sub_idx, target_date in enumerate(target_date_list, start=1):
            open_status = get_us_market_status(target_date)
            if open_status:
                concat_and_save_stock_dataframe(market_data_path, stock_code_dict_data, target_date)
                print(f"complete backfill - target_date : {target_date}, sub_idx : {sub_idx} ")
            else:
                print(f"Market is cloded {open_status}")

            if (sub_idx % 5) == 0:
                delay_seconds = random.randint(2, 5)

                print(f"Waiting for {delay_seconds} seconds...")
                time.sleep(delay_seconds)  # 선택된 초만큼 대기

                print("Re-Start!")

        big_delay_seconds = random.randint(30, 40)
        time.sleep(big_delay_seconds)  # 선택된 초만큼 대기

        print(f"Done {year_month} Month")

    print(f"Done all task")