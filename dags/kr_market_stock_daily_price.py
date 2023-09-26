import os
import pandas as pd
import requests
import pytz
import glob
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from config_generate import GetKoreaMarketOpenStatus
from datetime import datetime, timedelta

# TODO : 전체적인 코드 리팩토링 필요 - 간결, 중복 제거


class GetDailyStockData(GetKoreaMarketOpenStatus):
    def __init__(self):
        super().__init__()
        self.target_directory_path = self.check_stock_data_directory()

    def check_stock_data_directory(self):
        target_directory_name = "airflow/data"
        parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
        target_directory_path = os.path.join(parent_directory_path, target_directory_name)
        os.makedirs(target_directory_path, exist_ok=True)  # 존재할 경우 무시

        return target_directory_path

    def preprocessing_csv_to_dict(self, file_path, file_name):
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

    def read_stock_code_data(self):
        self.target_stock_code_dict = {}
        code_csv_file_list = glob.glob(f"{self.target_directory_path}{os.sep}*.csv")
        delimiter = os.sep

        for idx, file_path in enumerate(code_csv_file_list):
            file_name = file_path.split(delimiter)[-1]
            result = self.preprocessing_csv_to_dict(file_path, file_name)
            self.target_stock_code_dict.update(result)
            os.remove(file_path)

        return self.target_stock_code_dict

    def get_stock_data_json(self, stock_code, target_date):
        """
        한국 주식 조회하기 위한 코드 - parameter 값을 입력 하면 해당 기간의 종목 정보를 가져옵니다.
        하루치 데이터만 가져오는 것이 목표이므로, 조회 시작 날짜와 조회 마지막 날짜가 동일합니다.
        :param stock_code: 종목 코드 ex) 005930 : 삼성전자
        :param target_date: 조회 날짜 ex) 20230501
        :return: stock price json
        """

        url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
        path = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        url = f"{url_base}/{path}"

        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.key,
            "appSecret": self.secret_key,
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
        self.res_json = res.json()

        return self.res_json

    def convert_json_to_csv(self, stock_json, stock_info, group_code, target_date):
        fs_list = ['매출액', '영업이익', '당기순이익', 'ROE']  # financial_statmenet_metric_list
        revenue = stock_info[0][fs_list[0]]
        operating_profit = stock_info[0][fs_list[1]]
        net_income = stock_info[0][fs_list[2]]
        roe = stock_info[0][fs_list[3]]
        hts_kor_isnm = stock_json['output1']['hts_kor_isnm']  # HTS 한글 종목명

        output_one = stock_json['output1']  # output1 전체
        output_two = stock_json['output2']  # output2 전체

        stck_shrn_iscd = output_one['stck_shrn_iscd']  # 주식 단축 종목 코드

        output_one_df = pd.DataFrame([output_one])
        output_two_df = pd.DataFrame(output_two)

        drop_column_list = [
            'prdy_vrss', 'prdy_vrss_sign', 'acml_vol', 'acml_tr_pbmn',
            'stck_oprc', 'stck_hgpr', 'stck_lwpr'
        ]  # 중복 컬럼 사전 제거
        output_one_df = output_one_df.drop(drop_column_list, axis=1)

        daily_stock_data_df = pd.concat([output_two_df, output_one_df], axis=1)
        daily_stock_data_df['stck_bsop_date'] = daily_stock_data_df['stck_bsop_date'].apply(
            lambda x: pd.to_datetime(str(x), format='%Y%m%d')
        )
        daily_stock_data_df['group_code'] = group_code
        daily_stock_data_df['revenue'] = revenue
        daily_stock_data_df['operating_profit'] = operating_profit
        daily_stock_data_df['net_income'] = net_income
        daily_stock_data_df['roe'] = roe

        return daily_stock_data_df

    def concat_stock_dataframe(self, stock_info_dict, target_date):
        error_count = 0
        dfs = []

        for stock_info in stock_info_dict.values():
            print(f"Download {stock_info} data - Date : {target_date}")
            stock_code = stock_info[0]['단축코드']
            stock_group_code = stock_info[0]['그룹코드']
            stock_json = self.get_stock_data_json(stock_code, target_date)

            try:
                result_df = self.convert_json_to_csv(
                    stock_json, stock_info, stock_group_code, target_date
                )
                print(f"Done {stock_code} data - Date : {target_date}")
                dfs.append(result_df)

            except Exception as e:
                print(f"error : {e}")
                error_count += 1

            print("--------------------------------------------------")
            time.sleep(0.1)

        print(f"error count : {error_count}")
        daily_stock_data_df = pd.concat(dfs, ignore_index=True)

        csv_file_name = f"{self.target_directory_path}{os.sep}" \
                        f"kr_market_{target_date}_{target_date}.csv"
        daily_stock_data_df.to_csv(csv_file_name, index=False)

    def get_slack_message_token(self):
        slack_message_token = self.config_object.get('SLACK_CONFIG', 'channel_access_token')

        return slack_message_token

    def get_slack_channel_name(self):
        channel_name = self.config_object.get('SLACK_CONFIG', 'channel_name')
        slack_channel_name = f"#{channel_name}"

        return slack_channel_name

    def make_slack_message(self, **kwargs):
        dag_id = kwargs['dag'].dag_id
        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        slack_message = f"""
        **결과 요약**
        - DAG 작업: {dag_id}
        - 작업 날짜 : {target_date}
        - kst_data_interval_end : {kst_data_interval_end}
        """

        task_instance = kwargs['ti']
        task_instance.xcom_push(
            key="slack_message",
            value=slack_message
        )

get_daily_stock_data = GetDailyStockData()


def convert_utc_to_kst(utc_time):
    kst_timezone = pytz.timezone('Asia/Seoul')
    kst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(kst_timezone)

    return kst_time


with DAG(
    dag_id="kr_market_stock_daily_price_download",
    start_date= datetime(2023, 9, 25, 9),
    schedule_interval='40 8 * * 1-5',  # 한국 기준 월 - 금
    # default_args=default_args,
    catchup=False
) as dag:
    def get_market_open_status(**kwargs):
        check_market_open_status = GetKoreaMarketOpenStatus()

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        print(f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
              f' kwargs["data_interval_end"].strftime("%Y%m%d") : '
              f' {kwargs["data_interval_end"].strftime("%Y%m%d")}'
              f' kst_data_interval_end : {kst_data_interval_end}'
              f' target_date : {target_date}')

        open_yn_result = check_market_open_status.get_kr_market_status(target_date)
        kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)

    def check_open_status(**kwargs):
        open_yn_result = kwargs["ti"].xcom_pull(key="open_yn_result", task_ids="get_market_open_status")
        # open_yn_result = "N"

        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_open_task"

    def read_and_pass_stock_code_data(**kwargs):
        # TODO : 변수명 적절한 이름으로 리팩토링 하기
        stock_code_data = get_daily_stock_data.read_stock_code_data()
        kwargs["ti"].xcom_push(key="stock_code_data", value=stock_code_data)

        return stock_code_data

    def download_stock_data_csv(**kwargs):
        stock_info_dict = kwargs["ti"].xcom_pull(
            key="stock_code_data", task_ids='read_and_pass_stock_code'
        )

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")
        get_daily_stock_data.concat_stock_dataframe(stock_info_dict, target_date)

    def send_slack_message(**kwargs):
        dag_id = kwargs['dag'].dag_id
        # execution_date = kwargs['execution_date']

        data_interval_end = kwargs["data_interval_end"]
        kst_data_interval_end = convert_utc_to_kst(data_interval_end)
        target_date = kst_data_interval_end.strftime("%Y%m%d")

        slack_message = f"DAG execution completed! " \
                        f"DAG ID : {dag_id} " \
                        f"kst data interval end : {kst_data_interval_end} " \
                        f"target_date : {target_date}"

        task_instance = kwargs['ti']
        task_instance.xcom_push(
            key="slack_message",
            value=slack_message
        )

    get_market_open_status = PythonOperator(
        task_id="get_market_open_status",
        python_callable=get_market_open_status,
        provide_context=True
    )

    check_open_status = BranchPythonOperator(
        task_id="check_open_status",
        python_callable=check_open_status,
        provide_context=True
    )

    read_and_pass_stock_code_task = PythonOperator(
        task_id="read_and_pass_stock_code",
        python_callable=read_and_pass_stock_code_data,
        provide_context=True
    )

    download_stock_data_csv_task = PythonOperator(
        task_id="download_stock_data_csv",
        python_callable=download_stock_data_csv,
        provide_context=True
    )

    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_open_task = EmptyOperator(task_id="market_open_task")

    make_slack_message_task = PythonOperator(
        task_id="make_slack_message",
        python_callable=get_daily_stock_data.make_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token=get_daily_stock_data.get_slack_message_token(),
        channel=get_daily_stock_data.get_slack_channel_name(),
        text="{{ ti.xcom_pull(task_ids='make_slack_message', key='slack_message') }}",
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    get_market_open_status >> check_open_status
    check_open_status >> market_closed_task >> done_task
    check_open_status >> market_open_task
    market_open_task >> read_and_pass_stock_code_task >> download_stock_data_csv_task
    download_stock_data_csv_task >> done_task
    done_task >> make_slack_message_task >> slack_notification_task



