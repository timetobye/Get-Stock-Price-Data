import configparser
import requests
import json
import os
import pytz
import exchange_calendars as xcals

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta


"""
2023-08-01 기준 
- 한국투자증권에서 제공하는 API에는 토큰이 유효한 토큰인지 여부를 확인할 수 있는 API 는 제공하지 않은 상태
- 또한 토큰의 유효 기간이 24시간 이므로, 재발급 받는 것이 작업의 복잡성을 낮추는 것으로 판단
- 향후 API 가 유효한지 확인할 수 있는 환경이 갖추어 진다면, DAG 구성은 달라질 수 있음
"""
# TODO : Configuration 과 Market Status 코드를 분리할 계획 - 마켓 상태는 사실 데이터 가져오는 부분하고 연결해서 처리해야 한다. 현재는 임시코드
# TODO : code refactoring - file path, generate configuration files, market status(kr, us)


class ConfigurationControl:
    def __init__(self):
        self.config_ini_file_name = "config.ini"
        self.configuration_directory_name = "airflow/config"
        self.config_file_path = self.get_config_file_path()
        self.config_object = self.get_config_object()
        self.app_key, self.app_secret_key = self.get_application_keys()

    def get_config_file_path(self):
        configuration_directory = os.path.abspath(os.path.join(os.getcwd(), ".."))
        config_file_path = os.path.join(
            configuration_directory,
            self.configuration_directory_name,
            self.config_ini_file_name
        )
        print(f"self.config_file_path : {config_file_path}")

        return config_file_path

    def get_config_object(self):
        config_object = configparser.ConfigParser()
        config_object.read(self.config_file_path)

        return config_object

    def get_application_keys(self):
        app_key = self.config_object.get('LIVE_APP', 'KEY')
        app_secret_key = self.config_object.get('LIVE_APP', 'SECRET_KEY')

        return app_key, app_secret_key

    def generate_token(self):
        """
        token 을 새롭게 발생하는 코드 입니다. 반드시 필요합니다.
        참고 문서 : https://apiportal.koreainvestment.com/apiservice/oauth2#L_fa778c98-f68d-451e-8fff-b1c6bfe5cd30
        """
        url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
        path = "oauth2/tokenP"

        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret_key
        }
        post_url = f"{url_base}/{path}"

        res = requests.post(post_url, headers=headers, data=json.dumps(body))
        access_token = res.json()["access_token"]

        return access_token

    def update_token_file(self, access_token):
        """
        config.ini 파일에 새롭게 발급한 token 정보를 갱신 합니다.
        """
        self.config_object.set('LIVE_APP', 'ACCESS_TOKEN', access_token)

        with open(self.config_file_path, 'w') as configfile:
            self.config_object.write(configfile)

    def get_token(self):
        """ 발급한 토큰을 사용하는 경우"""
        token = self.config_object.get('LIVE_APP', 'ACCESS_TOKEN')

        return token

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

        url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain
        headers = {
            'content-Type': 'application/json',
            'appKey': self.app_key,
            'appSecret': self.app_secret_key
        }

        path = "uapi/hashkey"
        hash_url = f"{url_base}/{path}"

        res = requests.post(hash_url, headers=headers, data=json.dumps(datas))
        hash_key = res.json()["HASH"]

        return hash_key

    def update_hash_key_file(self, hash_key):
        """
        config.ini 파일에 새롭게 발급한 hash key 정보를 갱신 합니다.
        """
        self.config_object.set('LIVE_APP', 'HASH', str(hash_key))
        with open(self.config_file_path, 'w') as configfile:
            self.config_object.write(configfile)

    def get_hashkey(self):
        hashkey = self.config_object.get('LIVE_APP', 'HASH')

        return hashkey


class GetKoreaMarketOpenStatus(ConfigurationControl):
    """
    한국 마켓이 개장일인지 확인 하는 코드 입니다. 한국투자증권 API를 이용합니다.
    - 아래 답변 출처는 Q&A 페이지
    *영업일(Business Day): 은행이나 증권사 등 금융기관이 업무를 하는 날을 말합니다. 일반적으로 주말 및 법정공휴일은 영업일이 아닙니다.
    *거래일(Trading Day): 증권 업무가 가능한 날을 말합니다. 입출금, 이체 등의 업무가 포함됩니다.
    *개장일(Opening Day): 주식시장이 개장되는 날을 말합니다. 즉, 거래가 가능한 날이며, 이 역시 영업일과 다른 개념입니다.
    *결제일(Settlement Date): 주식 거래에서 실제로 주식을 인수하고 돈을 지불하는 날을 말합니다.
    보통 거래일로부터 2일 후가 결제일이 됩니다. 예를 들어, 4월 1일에 주식을 매수한 경우, 결제일은 4월 3일이 됩니다.
    """
    def __init__(self):
        super().__init__()
        self.key, self.secret_key, self.token = self.get_config()

    def get_config(self):
        key, secret_key = self.get_application_keys()
        token = self.config_object.get('LIVE_APP', 'ACCESS_TOKEN')

        return key, secret_key, token

    def get_kr_market_status(self, target_date):
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


class GetUSAMarketOpenStatus(ConfigurationControl):
    """
    미국 장이 개장일인지 확인하는 코드 입니다. 한투에서 별도 제공을 해주고 있지 않기 때문에 Open Source exchange_calendars 를 사용합니다.
    - https://github.com/gerrymanoim/exchange_calendars
    """
    def __init__(self):
        super().__init__()
        self.key, self.secret_key, self.token = self.get_config()

    def get_config(self):
        key, secret_key = self.get_application_keys()
        token = self.config_object.get('LIVE_APP', 'ACCESS_TOKEN')

        return key, secret_key, token

    def get_us_market_status(self, target_date):
        """
        return True or False
        """
        xnys = xcals.get_calendar("XNYS")
        open_status = xnys.is_session(target_date)
        
        return open_status


config_control = ConfigurationControl()
# TODO : 향후 국가별 거래가능 여부를 확인할 때 Branch 분기를 타거나 별도 처리할 부분. 2023년 09월 기준 임시 유지
check_market_open_status = GetKoreaMarketOpenStatus()


def convert_utc_to_kst(utc_time):
    kst_timezone = pytz.timezone('Asia/Seoul')
    kst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(kst_timezone)

    return kst_time


def check_weekday(**kwargs):
    data_interval_end = kwargs["data_interval_end"]
    kst_data_interval_end = convert_utc_to_kst(data_interval_end)

    print(f' kwargs["data_interval_start"] : {kwargs["data_interval_start"]}'
          f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
          f' kst_data_interval_end : {kst_data_interval_end}')

    weekday = kst_data_interval_end.strftime("%A")
    print(f"weekday : {weekday}")

    target_weekday_list = [
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'
    ]

    if weekday in target_weekday_list:
        return 'generate_token'
    else:
        return 'market_closed_task'


def generate_token_task(config_control, **kwargs):
    access_token = config_control.generate_token()
    kwargs["ti"].xcom_push(
        key="access_token",
        value=access_token
    )


def update_token_task(config_control, **kwargs):
    access_token = kwargs["ti"].xcom_pull(
        key="access_token",
        task_ids="generate_token"
    )
    # config_control.access_token = access_token
    config_control.update_token_file(access_token)


def generate_hash_key_task(config_control, **kwargs):
    hash_key = config_control.generate_hash_key()
    kwargs["ti"].xcom_push(
        key="hash_key",
        value=hash_key
    )


def update_hash_key_task(config_control, **kwargs):
    hash_key = kwargs["ti"].xcom_pull(
        key="hash_key",
        task_ids="generate_hash_key"
    )
    config_control.update_hash_key_file(hash_key)


def get_market_open_status(**kwargs):
    data_interval_end = kwargs["data_interval_end"]
    kst_data_interval_end = convert_utc_to_kst(data_interval_end)
    print(f' kwargs["data_interval_start"] : {kwargs["data_interval_start"]}'
          f' kwargs["data_interval_end"] : {kwargs["data_interval_end"]}'
          f' kst_data_interval_end : {kst_data_interval_end}')

    target_date = kst_data_interval_end.strftime("%Y%m%d")
    print(f"target_date : {target_date}")

    open_yn_result = check_market_open_status.get_kr_market_status(target_date)
    kwargs["ti"].xcom_push(key="open_yn_result", value=open_yn_result)


def check_kr_open_status(**kwargs):
    open_yn_result = kwargs["ti"].xcom_pull(
        key="open_yn_result",
        task_ids="get_market_open_status"
    )
    # open_yn_result = "N"  # temporarily test

    if open_yn_result == "N":
        return "market_closed_task"
    else:
        return "market_open_task"


with DAG(
    dag_id="generate_confing_and_check_market_status",
    start_date= datetime(2023, 9, 25, 17),
    schedule_interval='0 17 * * *',  # 한국 시간으로 새벽 2시 실행
    # default_args=default_args
    catchup=False,
    tags=["Config", "Key", "Token", "Mon - Fri"]

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
        provide_context=True
    )

    check_kr_open_status = BranchPythonOperator(
        task_id="check_kr_open_status",
        python_callable=check_kr_open_status,
        provide_context=True
    )

    task_for_Y = EmptyOperator(task_id="task_for_Y")
    task_for_N = EmptyOperator(task_id="task_for_N")

    # flow 의 명확성을 위해 EmptyOperator 사용
    market_closed_task = EmptyOperator(task_id="market_closed_task")
    market_open_task = EmptyOperator(task_id="market_open_task")

    done_task = EmptyOperator(task_id="done_task", trigger_rule="none_failed")

    check_weekday_task >> done_task
    check_weekday_task >> generate_token

    generate_token >> update_token >> generate_hask_key >> update_hask_key >> get_market_open_status >> check_kr_open_status

    check_kr_open_status >> market_open_task >> task_for_Y >> done_task
    check_kr_open_status >> market_closed_task >> task_for_N >> done_task