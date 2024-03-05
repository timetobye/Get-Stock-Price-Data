import requests
import exchange_calendars as xcals


class GetKoreaMarketOpenStatus:
    def __init__(self):
        self.hantoo_config_json = self._get_config_json()
        self.app_key = self.hantoo_config_json["config"]["key"]
        self.app_secret_key = self.hantoo_config_json["config"]["secret_key"]
        self.token = self.hantoo_config_json["config"]["access_token"]

    def _get_config_json(self):
        from airflow.models import Variable
        hantoo_config_json = Variable.get(key="hantoo", deserialize_json=True)

        return hantoo_config_json

    def get_kr_market_status(self, target_date):
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
