import argparse
import configparser
import json
import os
import pandas as pd
import requests


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


def get_period_data(stock_code: str, target_date: str) -> json:
    """
    한국 주식 조회하기 위한 코드 - parameter 값을 입력하면 해당 기간의 종목 정보를 가져옵니다.
    하루치 데이터만 가져오는 것이 목표이므로, 조회 시작 날짜와 조회 마지막 날짜가 동일합니다.
    :param stock_code: 종목 코드 ex) 005930 : 삼성전자
    :param target_date: 조회 날짜 ex) 20230501
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
        "fid_input_date_1": target_date,  # 조회 시작 날짜
        "fid_input_date_2": target_date,  # 조회 종료 일
        "fid_period_div_code": "D",  # 기간분류코드 D:일봉, W:주봉, M:월봉, Y:년봉
        "fid_org_adj_prc": "0",
    }

    res = requests.get(url, headers=headers, params=params)

    return res.json()


def convert_json_to_csv(json_data, target_date):
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
    csv_file_name = f"{current_directory_path}{os.sep}{target_directory_name}{os.sep}" \
                    f"{stck_shrn_iscd}_{target_date}_{target_date}.csv"
    daily_stock_data_df.to_csv(csv_file_name, index=False)


def get_market_status(target_date: str) -> json:
    key, secret_key, token = get_config()
    url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
    path = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    url = f"{url_base}/{path}"

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appKey": key,
        "appSecret": secret_key,
        "tr_id": "CTCA0903R",
        "custtype": "P"
    }

    params = {
        "BASS_DT": target_date,  # 기준일자(YYYYMMDD)
        "CTX_AREA_NK": "",  # 공백으로 입력
        "CTX_AREA_FK": ""  # 공백으로 입력
    }

    res = requests.get(url, headers=headers, params=params)
    opnd_yn_result = res.json()['output'][0]['opnd_yn']

    return opnd_yn_result


if __name__ == "__main__":
    """
    005930 : 삼성전자, 086520 : 에코프로, 035900 : JYP
    단일 항목일 경우 : python3 stock_daily_price.py --stock_code 005930 --target_date 20230530
    다중 항목일 경우 : python3 stock_daily_price.py --stock_code 005930 086520 035900 --target_date 20230530 
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
        "--target_date",
        type=str,
        default=None,
        help="Input date",
        required=True
    )

    args = arg_parser.parse_args()
    target_stock_code_list = args.stock_code
    target_date = args.target_date
    open_status = get_market_status(target_date)

    if target_stock_code_list is None or target_date is None:
        raise ValueError("Please Input Values")
    elif open_status == "N":
        raise "Market is closed"
    else:
        for target_stock_code in target_stock_code_list:
            stock_period_json_data = get_period_data(target_stock_code, target_date)
            convert_json_to_csv(stock_period_json_data, target_date)
