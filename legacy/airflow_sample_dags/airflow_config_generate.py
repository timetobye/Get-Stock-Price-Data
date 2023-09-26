import configparser
import requests
import json
import os
import pytz

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime

"""
2023-08-01 기준 
- 한국투자증권에서 제공하는 API에는 토큰이 유효한 토큰인지 여부를 확인할 수 있는 API 는 제공하지 않은 상태
- 또한 토큰의 유효 기간이 24시간 이므로, 재발급 받는 것이 작업의 복잡성을 낮추는 것으로 판단
- 향후 API 가 유효한지 확인할 수 있는 환경이 갖추어 진다면, DAG 구성은 달라질 수 있음
- EX) 토큰이 유효 한지 우선 확인 후 토큰 발급 여부를 결정 -> 진행 
"""


class ConfigurationControl:
    def __init__(self):
        self.config_ini_file_name = "config.ini"
        self.configuration_directory_name = "airflow/config"  # airflow docker volume mount
        self.get_config_file_path()
        self.get_config_object()

    def get_config_file_path(self):
        configuration_directory = os.path.abspath(os.path.join(os.getcwd(), "../.."))
        self.config_file_path = os.path.join(
            configuration_directory,
            self.configuration_directory_name,
            self.config_ini_file_name
        )
        print(f"self.config_file_path : {self.config_file_path}")

        return self.config_file_path

    def get_config_object(self):
        self.config_object = configparser.ConfigParser()
        self.config_object.read(self.config_file_path)

        return self.config_object

    def generate_token(self):
        """
        token 을 새롭게 발생하는 코드 입니다.
        참고 문서 : https://apiportal.koreainvestment.com/apiservice/oauth2#L_fa778c98-f68d-451e-8fff-b1c6bfe5cd30
        """
        app_key = self.config_object.get('LIVE_APP', 'KEY')
        app_secret_key = self.config_object.get('LIVE_APP', 'SECRET_KEY')

        url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
        path = "oauth2/tokenP"

        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret_key
        }
        post_url = f"{url_base}/{path}"

        res = requests.post(post_url, headers=headers, data=json.dumps(body))
        self.access_token = res.json()["access_token"]

        return self.access_token

    def update_token_file(self):
        """
        config.ini 파일에 새롭게 발급한 token 정보를 갱신 합니다.
        """
        self.config_object.set('LIVE_APP', 'ACCESS_TOKEN', self.access_token)

        with open(self.config_file_path, 'w') as configfile:
            self.config_object.write(configfile)

    def generate_hash_key(self):
        """
        hash key 를 새롭게 발행 하는 코드 입니다.
        """
        datas = {
            "CANO": '00000000',
            "ACNT_PRDT_CD": "01",
            "OVRS_EXCG_CD": "SHAA",
            "PDNO": "00001",
            "ORD_QTY": "500",
            "OVRS_ORD_UNPR": "52.65",
            "ORD_SVR_DVSN_CD": "0"
        }

        app_key = self.config_object.get('LIVE_APP', 'KEY')
        app_secret_key = self.config_object.get('LIVE_APP', 'SECRET_KEY')
        url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain

        headers = {
            'content-Type': 'application/json',
            'appKey': app_key,
            'appSecret': app_secret_key
        }

        path = "uapi/hashkey"
        hash_url = f"{url_base}/{path}"

        res = requests.post(hash_url, headers=headers, data=json.dumps(datas))
        self.hash_key = res.json()["HASH"]

        return self.hash_key

    def update_hash_key_file(self):
        """
        config.ini 파일에 새롭게 발급한 hash key 정보를 갱신 합니다.
        """
        self.config_object.set('LIVE_APP', 'HASH', str(self.hash_key))
        with open(self.config_file_path, 'w') as configfile:
            self.config_object.write(configfile)


class GetMarketOpenStatus(ConfigurationControl):
    def __init__(self):
        super().__init__()
        self.get_config()

    def get_config(self):
        self.key = self.config_object.get('LIVE_APP', 'KEY')
        self.secret_key = self.config_object.get('LIVE_APP', 'SECRET_KEY')
        self.token = self.config_object.get('LIVE_APP', 'ACCESS_TOKEN')

    def get_market_status(self, target_date: str) -> json:
        # key, secret_key, token = get_config()
        url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
        path = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        url = f"{url_base}/{path}"

        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.key,
            "appSecret": self.secret_key,
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
        print(f"opnd_yn_result : {opnd_yn_result}")

        return opnd_yn_result


config_control = ConfigurationControl()
check_market_open_status = GetMarketOpenStatus()


def convert_utc_to_kst(utc_time):
    kst_timezone = pytz.timezone('Asia/Seoul')
    kst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(kst_timezone)

    return kst_time


def check_weekday(**kwargs):
    data_interval_end = kwargs["data_interval_end"]
    kst_data_interval_end = convert_utc_to_kst(data_interval_end)

    weekday = kst_data_interval_end.strftime("%A")
    target_weekday_list = [
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'
    ]
    if weekday in target_weekday_list:
        return 'generate_token'
    else:
        return 'market_closed_task'


def generate_token_task(config_control, **kwargs):
    config_control.generate_token()
    kwargs["ti"].xcom_push(
        key="access_token",
        value=config_control.access_token
    )


def update_token_task(config_control, **kwargs):
    access_token = kwargs["ti"].xcom_pull(
        key="access_token",
        task_ids="generate_token"
    )
    config_control.access_token = access_token
    config_control.update_token_file()


def generate_hash_key_task(config_control, **kwargs):
    config_control.generate_hash_key()
    kwargs["ti"].xcom_push(
        key="hash_key",
        value=config_control.hash_key
    )


def update_hash_key_task(config_control, **kwargs):
    hash_key = kwargs["ti"].xcom_pull(
        key="hash_key",
        task_ids="generate_hash_key"
    )
    print(f"type : {type(hash_key)}, {hash_key}")
    config_control.hash_key = hash_key
    config_control.update_hash_key_file()


def get_market_open_status(config_control, **kwargs):
    data_interval_end = kwargs["data_interval_end"]
    kst_data_interval_end = convert_utc_to_kst(data_interval_end)
    target_date = kst_data_interval_end.strftime("%Y%m%d")
    open_yn_result = check_market_open_status.get_market_status(target_date)
    kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)


def check_open_status(**kwargs):
    open_yn_result = kwargs["ti"].xcom_pull(key="open_yn_result", task_ids="get_market_open_status")
    # open_yn_result = "N"

    if open_yn_result == "N":
        return "market_closed_task"
    else:
        return "market_open_task"


with DAG(
    dag_id="generate_token_and_hashkey",
    start_date= datetime(2023, 8, 18, 8),
    schedule_interval='0 17 * * 0-5', # UTC 기준 일 - 금(한국 기준 월 - 토)
    # default_args=default_args
    catchup=False,
    tags=["Token", "HashKey", "Mon - Sat"]

) as dag:
    check_weekday_task = BranchPythonOperator(
        task_id="check_weekday_task",
        python_callable=check_weekday,
        provide_context=True
    )

    generate_token = PythonOperator(
        task_id="generate_token",
        python_callable=generate_token_task,
        op_args=[config_control],
        provide_context=True

    )

    update_token = PythonOperator(
        task_id="update_token",
        python_callable=update_token_task,
        op_args=[config_control],
        provide_context=True
    )

    generate_hask_key = PythonOperator(
        task_id="generate_hask_key",
        python_callable=generate_hash_key_task,
        op_args=[config_control],
        provide_context=True

    )

    update_hask_key = PythonOperator(
        task_id="update_hask_key",
        python_callable=update_hash_key_task,
        op_args=[config_control],
        provide_context=True
    )

    get_market_open_status = PythonOperator(
        task_id="get_market_open_status",
        python_callable=get_market_open_status,
        op_args=[config_control],
        provide_context=True
    )

    check_open_status = BranchPythonOperator(
        task_id="check_open_status",
        python_callable=check_open_status,
        provide_context=True
    )

    task_for_Y = EmptyOperator(task_id="task_for_Y")

    # flow 의 명확성을 위해 EmptyOperator 사용
    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_open_task = EmptyOperator(task_id="market_open_task")

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    check_weekday_task >> done_task
    check_weekday_task >> generate_token

    generate_token >> update_token >> generate_hask_key >> update_hask_key >> get_market_open_status >> check_open_status

    check_open_status >> market_open_task >> task_for_Y >> done_task
    check_open_status >> market_closed_task >> done_task