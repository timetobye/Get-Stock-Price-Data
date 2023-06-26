import argparse
import calendar
import configparser
import json
import os
import pandas as pd
import requests
import time

from datetime import date, datetime, timedelta


def get_config_object():
    config_ini_file_name = "config.ini"
    config_ini_file_path = f"configuration{os.sep}{config_ini_file_name}"

    config_object = configparser.ConfigParser()
    config_object.read(config_ini_file_path)

    return config_object


def get_config():
    config_object = get_config_object()
    app_key = config_object.get('LIVE_APP', 'KEY')
    app_secret_key = config_object.get('LIVE_APP', 'SECRET_KEY')
    app_token = config_object.get('LIVE_APP', 'ACCESS_TOKEN')

    return app_key, app_secret_key, app_token


def get_period_data(stock_code: str, start_date: str, end_date: str) -> json:
    """
    한국 주식 조회하기 위한 코드 - parameter 값을 입력하면 해당 기간의 종목 정보를 가져옵니다.
    :param stock_code: 종목 코드 ex) 005930 : 삼성전자
    :param start_date: 조회 시작 날짜 ex) 20230501
    :param end_date: 조회 마지막 날짜 ex) 20230504
    :return: stock price json
    """
    key, secret_key, token = get_config()
    url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
    path = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    url = f"{url_base}/{path}"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appKey": key,
        "appSecret": secret_key,
        "tr_id": "FHKST03010100"
    }

    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": stock_code,  # "005930" : 삼성전자
        "fid_input_date_1": start_date,  # 조회 시작 날짜
        "fid_input_date_2": end_date,  # 조회 종료 일
        "fid_period_div_code": "D",  # 기간분류코드 D:일봉, W:주봉, M:월봉, Y:년봉
        "fid_org_adj_prc": "0",
    }

    res = requests.get(url, headers=headers, params=params)

    return res.json()


def convert_json_to_csv(json_data, start_date, end_date):
    hts_kor_isnm = json_data['output1']['hts_kor_isnm']  # HTS 한글 종목명
    stck_shrn_iscd = json_data['output1']['stck_shrn_iscd']  # 주식 단축 종목코드
    daily_stock_data_result = json_data['output2']  # output2 전체

    daily_stock_data_df = pd.DataFrame(daily_stock_data_result)
    daily_stock_data_df['stck_bsop_date'] = daily_stock_data_df['stck_bsop_date'].apply(
        lambda x: pd.to_datetime(str(x), format='%Y%m%d')
    )
    daily_stock_data_df['hts_kor_isnm'] = hts_kor_isnm
    daily_stock_data_df['stck_shrn_iscd'] = stck_shrn_iscd

    current_directory_path = os.getcwd()
    target_directory_name = "stock_csv_files"
    target_directory_path = f"{current_directory_path}{os.sep}{target_directory_name}"

    isdir = os.path.isdir(target_directory_path)
    if not isdir:
        os.mkdir(target_directory_path)

    csv_file_name = f"{target_directory_path}{os.sep}" \
                    f"{stck_shrn_iscd}_{start_date}_{end_date}.csv"
    daily_stock_data_df.to_csv(csv_file_name, index=False)


def target_year_month_list(start_year_month, end_year_month):
    # 입력 받은 두 개의 yyyymm 값을 datetime 객체로 변환
    start_date = datetime.strptime(start_year_month, "%Y%m") # 202303 -> 2023-03-01 00:00:00
    end_date = datetime.strptime(end_year_month, "%Y%m")

    # start_date부터 end_date까지의 월별 값을 생성
    result = []
    while start_date <= end_date:
        result.append(start_date.strftime("%Y%m"))
        start_date = start_date + timedelta(days=32)
        start_date = start_date.replace(day=1)  # day 부분을 1로 변경

    return result


def get_start_and_end_date(target_year_month):
    today = datetime.date(datetime.today())

    input_date_str = target_year_month # ex) 202303
    year, month = int(input_date_str[:4]), int(input_date_str[4:])

    # 월의 첫 날짜
    first_day = date(year, month, 1)

    # 월의 마지막 날짜
    last_day = date(year, month, calendar.monthrange(year, month)[1])

    # 조회 하는 시점의 달은 마지막 날이 아닐 수 있으므로 분기를 타야 한다.
    if last_day > today:
        last_day = today - timedelta(1)
        first_day_str = first_day.strftime("%Y%m%d")
        last_day_str = last_day.strftime("%Y%m%d")
    else:
        # 문자열 형태로 출력
        first_day_str = first_day.strftime("%Y%m%d")
        last_day_str = last_day.strftime("%Y%m%d")

    return first_day_str, last_day_str


# '-l','--list', nargs='+'
if __name__ == "__main__":
    """
    종목 코드 예시 > 005930 : 삼성전자, 086520 : 에코프로, 035900 : JYP
    # 단일 종목에 대해서만 가져오는 경우
    python3 stock_target_period_price.py --stock_code 005930 --start_year_and_month 202206 --end_year_and_month 202305
    
    # 여러 종목을 한 번에 가져와야 하는 경우
    python3 stock_target_period_price.py --stock_code 005930 086520 035900 --start_year_and_month 202005 --end_year_and_month 202305 
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--stock_code",
        nargs='+',
        default=None,
        type=str,
        help="Input Stock Code",
        required=True
    )

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
    # target_stock_code = args.stock_code
    target_stock_code_list = args.stock_code
    target_start_year_and_month = args.start_year_and_month
    target_end_year_and_month = args.end_year_and_month

    if target_stock_code_list is None or target_start_year_and_month is None or target_end_year_and_month is None:
        raise ValueError("Please Input Values")
    else:
        year_month_list = target_year_month_list(target_start_year_and_month, target_end_year_and_month)

        for target_stock_code in target_stock_code_list:
            for idx, year_month in enumerate(year_month_list):
                target_start_date, target_end_date = get_start_and_end_date(year_month)
                stock_period_json_data = get_period_data(target_stock_code, target_start_date, target_end_date)
                convert_json_to_csv(stock_period_json_data, target_start_date, target_end_date)
                # pprint(stock_period_json_data)

                if idx % 12 == 0:
                    # API 호출 시 시간 딜레이 없이 할 경우 에러가 나는 경우가 종종 있다. 코드 오류보다는 응답 처리 하는 곳의 문제로 보임
                    # time.sleep(1) 을 줘서 해당 문제를 해결 하였다.
                    time.sleep(1)
                    print(f"time sleep : 1 seconds, {year_month}, {target_stock_code}")