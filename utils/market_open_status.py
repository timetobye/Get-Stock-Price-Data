import requests
import exchange_calendars as xcals
import sys
sys.path.append('/opt/airflow/')


class GetKoreaMarketOpenStatus:
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
        self.hantoo_config_json = self.get_config()
        self.app_key = self.hantoo_confing_json["config"]["key"]
        self.app_secret_key = self.hantoo_confing_json["config"]["secret_key"]
        self.token = self.hantoo_confing_json["config"]["access_token"]

    def get_config_json(self):
        from airflow.models import Variable
        hantoo_config_json = Variable.get(key="hantoo", deserialize_json=True)

        return hantoo_config_json

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


class USAMarketOpenStatus:
    """
    미국 장이 개장일인지 확인하는 코드 입니다. 한투에서 별도 제공을 해주고 있지 않기 때문에 Open Source exchange_calendars 를 사용합니다.
    - https://github.com/gerrymanoim/exchange_calendars
    """

    def __init__(self):
        pass

    @staticmethod
    def get_us_market_open_status(target_date):
        xnys = xcals.get_calendar("XNYS")
        open_status = xnys.is_session(target_date)  # True or False

        if open_status:
            return 'Y'
        else:
            return 'N'
