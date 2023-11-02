import configparser
import requests
import json
import os
import pytz
import exchange_calendars as xcals

from datetime import datetime, timedelta


class ConfigurationControl:
    def __init__(self):
        self.config_ini_file_name = "config.ini"
        # self.configuration_directory_name = "config"
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