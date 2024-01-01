import requests
import json


class GenerateHantooToken:
    """
    기존에 config.ini 파일을 읽는 방식에서 airflow 의 variable 을 이용하여 처리하는 방식으로 변경
    한국투자증권의 경우 Token 업데이트만 하면 현재 사용 범위에서 문제가 없으므로 코드를 줄이고 필요한 항목만 작성
    """
    def __init__(self):
        self.hantoo_confing_json = self.get_hantoo_config_json()
        self.app_key = self.hantoo_confing_json["config"]["key"]
        self.app_secret_key = self.hantoo_confing_json["config"]["secret_key"]

    def get_hantoo_config_json(self):
        from airflow.models import Variable
        hontoo_config_json = Variable.get(key="hantoo", deserialize_json=True)

        return hontoo_config_json

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

    def update_token(self, new_token):
        from airflow.models import Variable

        self.hantoo_confing_json["config"]["access_token"] = new_token
        Variable.set(key='hantoo', value=self.hantoo_confing_json, serialize_json=True)

    def get_token(self):
        """ 발급한 토큰을 사용하는 경우"""
        token = self.hantoo_confing_json["config"]["access_token"]

        return token
