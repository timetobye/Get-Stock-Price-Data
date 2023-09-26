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


def get_market_status(target_date):
    # key, secret_key, token = get_config()
    url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
    path = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    url = f"{url_base}/{path}"
    app_key, app_secret_key, app_token = get_config()

    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {app_token}",
        "appKey": app_key,
        "appSecret": app_secret_key,
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
    # print(f"opnd_yn_result : {opnd_yn_result}")

    return opnd_yn_result


def preprocessing_csv_to_dict(file_path, file_name):
    market_type = file_name.split("_")[0]
    origin_code_file_df = pd.read_csv(file_path, dtype={"단축코드": str, "시가총액": 'int64'})

    column_list = origin_code_file_df.columns.tolist()
    target_column = ['단축코드', '한글종목명', '그룹코드', '시가총액', '매출액', '영업이익', '당기순이익', 'ROE']
    target_code_file_df = origin_code_file_df[target_column]
    target_code_file_df['마켓'] = market_type

    target_code_file_df.sort_values(by="시가총액", ascending=False, inplace=True)
    target_code_file_df = target_code_file_df[target_code_file_df['시가총액'] > 0]

    result_dict = target_code_file_df.groupby('한글종목명').apply(
        lambda x: x.drop('한글종목명', axis=1).to_dict('records')
    ).to_dict()

    return result_dict


def read_stock_code_data(target_directory_path):
    target_stock_code_dict = {}
    code_csv_file_list = glob.glob(f"{target_directory_path}{os.sep}*.csv")
    delimiter = os.sep

    if not code_csv_file_list:
        print(f"code_csv_file_list is empty. Stopping execution")
        raise FileNotFoundError

    for idx, file_path in enumerate(code_csv_file_list):
        file_name = file_path.split(delimiter)[-1]
        result_dict = preprocessing_csv_to_dict(file_path, file_name)
        target_stock_code_dict.update(result_dict)

        os.remove(file_path)  # 파일 보존 시에는 주석 처리

    return target_stock_code_dict


def get_stock_data_json(stock_code: str, target_date) -> json:
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
        "fid_input_date_2": target_date,  # 조회 종료 일, 동일하게 설정
        "fid_period_div_code": "D",  # 기간분류코드 D:일봉, W:주봉, M:월봉, Y:년봉
        "fid_org_adj_prc": "0",
    }

    res = requests.get(url, headers=headers, params=params)

    return res.json()


def convert_json_to_csv(stock_json, stock_info, group_code, target_date):
    fs_list = ['매출액', '영업이익', '당기순이익', 'ROE']  # financial_statement_metric_list
    revenue = stock_info[0][fs_list[0]]
    operating_profit = stock_info[0][fs_list[1]]
    net_income = stock_info[0][fs_list[2]]
    roe = stock_info[0][fs_list[3]]

    output_one = stock_json['output1']  # output1 전체
    output_two = stock_json['output2']  # output2 전체

    output_one_df = pd.DataFrame([output_one])
    output_two_df = pd.DataFrame(output_two)

    drop_duplicated_column_list = [
        'prdy_vrss', 'prdy_vrss_sign', 'acml_vol', 'acml_tr_pbmn',
        'stck_oprc', 'stck_hgpr', 'stck_lwpr'
    ]  # 중복 컬럼 사전 제거
    output_one_df = output_one_df.drop(drop_duplicated_column_list, axis=1)

    daily_stock_data_df = pd.concat([output_two_df, output_one_df], axis=1)
    daily_stock_data_df['stck_bsop_date'] = daily_stock_data_df['stck_bsop_date'].apply(
        lambda x: pd.to_datetime(str(x), format='%Y%m%d')
    )

    # rev, operating_profit, net_income, roe 등은 거래소 마스터 파일을 통해 가져오는 것이기 때문에 별도 수정 불가
    # 만일 필요하다면 Dart 에서 API 를 통해 데이터를 가져와야 함
    daily_stock_data_df['group_code'] = group_code
    daily_stock_data_df['revenue'] = revenue
    daily_stock_data_df['operating_profit'] = operating_profit
    daily_stock_data_df['net_income'] = net_income
    daily_stock_data_df['roe'] = roe

    return daily_stock_data_df


def concat_and_save_stock_dataframe(target_directory_path, stock_info_dict, target_date):
    error_count = 0
    error_stock_code_data = {}
    dfs = []

    for stock_info in stock_info_dict.values():
        print(f"Download {stock_info} data - Date : {target_date}")

        stock_code = stock_info[0]['단축코드']
        stock_group_code = stock_info[0]['그룹코드']
        stock_json = get_stock_data_json(stock_code, target_date)

        try:
            result_df = convert_json_to_csv(
                stock_json, stock_info, stock_group_code, target_date
            )
            print(f"Done {stock_code} data - Date : {target_date}")
            dfs.append(result_df)

        except Exception as e:
            print(f"error : {e}")
            error_count += 1

            if stock_code not in error_stock_code_data:
                error_stock_code_data[stock_code] = target_date

        print("--------------------------------------------------")

        random_float = random.uniform(0.1, 0.2)  # 과도한 API 호출로 인한 Connection 차단 방지용
        time.sleep(random_float)

    print(f"target_date : {target_date}, error count : {error_count}")
    print(f"error_stock_code_data : {error_stock_code_data}")
    daily_stock_data_df = pd.concat(dfs, ignore_index=True)

    csv_file_name = f"{target_directory_path}{os.sep}" \
                    f"kr_market_{target_date}_{target_date}.csv"
    daily_stock_data_df.to_csv(csv_file_name, index=False)


def make_target_date_list(start_date_str, end_date_str):
    today = datetime.now()
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")

    date_list = []

    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() <= 4:
            if current_date <= today:
                date_list.append(current_date.strftime("%Y%m%d"))

        current_date += timedelta(days=1)

    return date_list


def get_data_directory_path():
    target_directory_name = "data"
    parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
    target_directory_path = os.path.join(parent_directory_path, target_directory_name)

    return target_directory_path


# '-l','--list', nargs='+'
if __name__ == "__main__":
    """
    # 일정 기간으로 지정해서 다운로드 하는 경우 - 일 단위로 받음
    python3 backfill_kr_market_stock_daily_price_day.py --start_date_str 20230901 --end_date_str 20230902
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--start_date_str",
        type=str,
        default=None,
        help="Input Date",
        required=True
    )

    arg_parser.add_argument(
        "--end_date_str",
        type=str,
        default=None,
        help="Input Date",
        required=True
    )

    args = arg_parser.parse_args()
    target_start_date_str = args.start_date_str
    target_end_date_str = args.end_date_str

    market_data_path = get_data_directory_path()
    print(f"market_data_path : {market_data_path}")
    stock_code_dict_data = read_stock_code_data(market_data_path)
    target_date_list = make_target_date_list(target_start_date_str, target_end_date_str)
    print(f"target_date_list : {target_date_list}")

    for idx, target_date in enumerate(target_date_list, start=1):
        open_status = get_market_status(target_date)
        if open_status == "Y":
            concat_and_save_stock_dataframe(market_data_path, stock_code_dict_data, target_date)
            print(f"complete backfill - target_date : {target_date}, idx : {idx} ")
        elif open_status == "N":
            print(f"Market is cloded")
        else:
            print(f"Unknown Status : {open_status}")

        if (idx % 5) == 0:
            delay_seconds = random.randint(2, 5)

            print(f"Waiting for {delay_seconds} seconds...")
            time.sleep(delay_seconds)  # 선택된 초만큼 대기

            print("Re-Start!")
