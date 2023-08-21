import os
import pandas as pd
import requests
import pytz

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from airflow_config_generate import GetMarketOpenStatus
from datetime import datetime, timedelta


class GetDailyStockData(GetMarketOpenStatus):
    def __init__(self):
        super().__init__()
        self.check_stock_data_directory()

    def check_stock_data_directory(self):
        target_directory_name = "airflow/data"  # airflow docker volume mount
        parent_directory_path = os.path.abspath(os.path.join(os.getcwd(), ".."))
        self.target_directory_path = os.path.join(parent_directory_path, target_directory_name)
        os.makedirs(self.target_directory_path, exist_ok=True)

    def read_stock_code_data(self):
        # 별도 파일에서 Stock_code 를 불러와야 함 - 코드 작업 진행 중
        target_stock_code_dict = {
            "삼성전자": "005930",
            "에코프로": "086520",
            "JYP": "035900"
        }

        return target_stock_code_dict

    def get_stock_data_json(self, stock_code, target_date):
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

    def convert_json_to_csv(self, target_date):
        hts_kor_isnm = self.res_json['output1']['hts_kor_isnm']  # HTS 한글 종목명
        stck_shrn_iscd = self.res_json['output1']['stck_shrn_iscd']  # 주식 단축 종목코드
        daily_stock_data_result = self.res_json['output2']  # output2 전체

        daily_stock_data_df = pd.DataFrame(daily_stock_data_result)
        daily_stock_data_df['stck_bsop_date'] = daily_stock_data_df['stck_bsop_date'].apply(
            lambda x: pd.to_datetime(str(x), format='%Y%m%d')
        )
        daily_stock_data_df['hts_kor_isnm'] = hts_kor_isnm
        daily_stock_data_df['stck_shrn_iscd'] = stck_shrn_iscd

        csv_file_name = f"{self.target_directory_path}{os.sep}" \
                        f"{stck_shrn_iscd}_{target_date}_{target_date}.csv"
        daily_stock_data_df.to_csv(csv_file_name, index=False)


get_daily_stock_data = GetDailyStockData()

with DAG(
    dag_id="stock_daily_price_ks_market",
    start_date= datetime(2023, 8, 13, 9),
    schedule_interval='0 9 * * 1-6',  # UTC 기준, interval 에 대한 이해 필요
    catchup=True
) as dag:
    def get_market_open_status(**kwargs):
        check_market_open_status = GetMarketOpenStatus()
        target_date = kwargs["execution_date"].strftime('%Y%m%d')
        print(f'kwargs["execution_date"].strftime("%Y%m%d") : {kwargs["execution_date"].strftime("%Y%m%d")}')

        open_yn_result = check_market_open_status.get_market_status(target_date)
        kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)

    def check_open_status(**kwargs):
        open_yn_result = kwargs["ti"].xcom_pull(key="open_yn_result", task_ids="get_market_open_status")
        if open_yn_result == "N":
            return "market_closed_task"
        else:
            return "market_open_task"

    def read_and_pass_stock_code_data(**kwargs):
        stock_code_data = get_daily_stock_data.read_stock_code_data()
        kwargs["ti"].xcom_push(key="stock_code_data", value=stock_code_data)

        return stock_code_data

    def download_stock_data_csv(**kwargs):
        stock_code_dict = kwargs["ti"].xcom_pull(
            key="stock_code_data", task_ids='read_and_pass_stock_code'
        )
        target_date = kwargs["execution_date"].strftime('%Y%m%d')
        print(f"stock_code_dict from xcom_pull : {stock_code_dict}")
        for stock_code in stock_code_dict.values():
            print(f"Download {stock_code} data - Date : {target_date}")

            get_daily_stock_data.get_stock_data_json(stock_code, target_date)
            get_daily_stock_data.convert_json_to_csv(target_date)

            print(f"Done {stock_code} data - Date : {target_date}")
            print("--------------------------------------------------")

    def send_slack_message(**kwargs):
        dag_id = kwargs['dag'].dag_id
        execution_date = kwargs['execution_date']

        slack_message = f"DAG execution completed! " \
                        f"DAG ID : {dag_id} " \
                        f"Execution Date: {execution_date}"

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

    send_slack_message_task = PythonOperator(
        task_id="send_slack_message",
        python_callable=send_slack_message,
        provide_context=True
    )

    slack_notification_task = SlackAPIPostOperator(
        task_id="slack_notification_task",
        token="your_token",
        channel="#your_channel",
        text="{{ ti.xcom_pull(task_ids='send_slack_message', key='slack_message') }}",  # jinja template 활용
    )

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    get_market_open_status >> check_open_status
    check_open_status >> market_closed_task >> done_task
    check_open_status >> market_open_task
    market_open_task >> read_and_pass_stock_code_task >> download_stock_data_csv_task
    download_stock_data_csv_task >> done_task
    done_task >> send_slack_message_task >> slack_notification_task


