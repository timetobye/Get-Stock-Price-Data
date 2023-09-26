import configparser
import requests
import json
import os


def get_config_object():
    config_ini_file_name = "config.ini"
    config_ini_file_path = f"configuration{os.sep}{config_ini_file_name}"

    config_object = configparser.ConfigParser()
    config_object.read(config_ini_file_path)

    return config_object


def generate_token():
    """
    token 을 새롭게 발생하는 코드 입니다.
    참고 문서 : https://apiportal.koreainvestment.com/apiservice/oauth2#L_fa778c98-f68d-451e-8fff-b1c6bfe5cd30
    """
    config_object = get_config_object()
    app_key = config_object.get('LIVE_APP', 'KEY')
    app_secret_key = config_object.get('LIVE_APP', 'SECRET_KEY')

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
    access_token = res.json()["access_token"]

    return access_token


def update_token_file(token_string):
    """
    config.ini 파일에 새롭게 발급한 token 정보를 갱신합니다.
    """
    config_object = get_config_object()
    config_object.set('LIVE_APP', 'ACCESS_TOKEN', token_string)

    config_ini_file_name = "config.ini"
    config_ini_file_path = f"configuration{os.sep}{config_ini_file_name}"

    with open(config_ini_file_path, 'w') as configfile:
        config_object.write(configfile)


def generate_hash_key():
    """
    hash key 를 새롭게 발생하는 코드 입니다.
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

    config_object = get_config_object()
    app_key = config_object.get('LIVE_APP', 'KEY')
    app_secret_key = config_object.get('LIVE_APP', 'SECRET_KEY')
    url_base = "https://openapi.koreainvestment.com:9443"  # 실전 Domain

    headers = {
        'content-Type': 'application/json',
        'appKey': app_key,
        'appSecret': app_secret_key
    }

    path = "uapi/hashkey"
    hash_url = f"{url_base}/{path}"

    res = requests.post(hash_url, headers=headers, data=json.dumps(datas))
    hash_key = res.json()["HASH"]

    return hash_key


def update_hash_key_file(hash_key):
    """
    config.ini 파일에 새롭게 발급한 hash key 정보를 갱신합니다.
    """
    config_object = get_config_object()
    config_object.set('LIVE_APP', 'HASH', hash_key)

    config_ini_file_name = "config.ini"
    config_ini_file_path = f"configuration{os.sep}{config_ini_file_name}"

    with open(config_ini_file_path, 'w') as configfile:
        config_object.write(configfile)


def main():
    # generate token & hash-key and update json configuration
    token = generate_token()
    update_token_file(token)
    hash_key = generate_hash_key()
    update_hash_key_file(hash_key)


if __name__ == '__main__':
    main()
