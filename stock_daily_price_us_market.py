import configparser
import os
import pandas as pd
import requests

from datetime import date, datetime, timedelta

"""
해외 거래소에 상장한 주가 정보를 가져오는 코드 입니다.

API 안내
- 다우30, 나스닥 100, S&P500에 속하지 않은 종목까지 가져오려면 공식 문서에서 안내하는 API 를 이용해야 합니다.
- API : https://apiportal.koreainvestment.com/apiservice/apiservice-domestic-stock-current#L_3eeac674-072d-4674-a5a7-f0ed01194a81
- 다우30, 나스닥 100, S&P500 에 속하는 것만 데이터를 가져올 경우는 아래 API 참고
- API : https://apiportal.koreainvestment.com/apiservice/apiservice-domestic-stock-current#L_da63a88a-e288-426f-9498-42db0b537bf3

데이터 안내
- 20230815 기준, 이 API 를 사용하면 최대 100 영업일에 해당하는 데이터를 가져옵니다.
- 하루치 데이터만 가져오는 것은 API 에서 기능을 제공하지 않으므로, JSON 내 output 리스트에서 첫 번째 항목만 가져와야 합니다.
- 기본 설정은 하루치 데이터만 가져오는 것을 목표로 합니다.

input 관련
- 한국 주식 조회와 다른 점은 거래소, 티커, 날짜를 입력해야 합니다.
- 거래소 및 티커 정보는 ticker directory 에 있는 코드를 참고
"""

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


def get_period_data(exchange_code, ticker, target_date):
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


def convert_multiple_data(data_result):
    return pd.DataFrame(data_result)


def convert_json_to_csv(json_data, target_date, period="daily"):
    # TODO: Airflow 만들 때, 한글명도 가져 오는 것 코드 작성하기. 현재 코드에서는 불필요.
    stock_rsym = json_data['output1']['rsym']
    stock_ticker = stock_rsym[4:]  # "D+시장구분(3자리)+종목코드 : DNASAAPL : D+NAS(나스닥)+AAPL(애플)"
    stock_exchange_code = stock_rsym[1:4]

    if period == "daily":
        daily_stock_data_df = convert_daily_data(json_data['output2'][0]) # output2 - 하루치 데이터만 가져오기, API에서 output 길이 조절 옵션 미제공
    elif period == "multiple":
        daily_stock_data_df = convert_multiple_data(json_data['output2']) # output2 - 최대 100건 가져오기
    else:
        # 둘 다 아닐 경우도 daily로 처리
        daily_stock_data_df = convert_daily_data(json_data['output2'][0])

    daily_stock_data_df['xymd'] = daily_stock_data_df['xymd'].apply(
        lambda x: pd.to_datetime(str(x), format='%Y%m%d')
    )
    daily_stock_data_df['stock_ticker'] = stock_ticker

    current_directory_path = os.getcwd()
    target_directory_name = "stock_csv_files"
    target_directory_path = f"{current_directory_path}{os.sep}{target_directory_name}"
    os.makedirs(target_directory_path, exist_ok=True)

    csv_file_name = f"{target_directory_path}{os.sep}" \
                    f"{stock_ticker}_{target_date}_{target_date}.csv"
    daily_stock_data_df.to_csv(csv_file_name, index=False)


def is_weekday(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        day_of_week = date_obj.weekday()

        return 0 <= day_of_week <= 4
    except ValueError:
        return False


def main():
    # Sample data
    exchange_code = "NAS"
    ticker = "AAPL"
    target_date = "20230814"
    period = "daily"
    # period = "multiple"

    if is_weekday(target_date):
        json_result = get_period_data(exchange_code, ticker, target_date)
        convert_json_to_csv(json_result, target_date, period)
    else:
        print("The stock market is closed. Take a rest!")


if __name__ == "__main__":
    main()