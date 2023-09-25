Get Stock Price Data with Airflow
===

한국투자증권 트레이딩 서비스 Open API와 Airflow를 이용하여 주가 데이터를 가져 옵니다.

| | |
| --- | --- |
| Env 1 | ![Static Badge](https://img.shields.io/badge/Python-v3.7+-red) ![Static Badge](https://img.shields.io/badge/airflow-v2.6.3-brightgreen)|
| Env 2 | ![Static Badge](https://img.shields.io/badge/docker-blue) ![Static Badge](https://img.shields.io/badge/docker_compose-yellow)|

### 1. 소개

한국투자증권 트레이딩 서비스 Open API 와 Airflow 를 이용하여 주가 데이터를 가져오는 Repo 입니다. 

소개와 관련한 자세한 사항은 [Wiki](https://github.com/timetobye/Get-Stock-Price-Data/wiki) 를 참고해주시기 바랍니다.

### 2. 설치 및 환경 구성

#### (1) Docker 설치

공식 사이트 : https://www.docker.com/

#### (2) Airflow Docker 설치
공식 사이트 : https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

안내된 과정에서 일부 추가 하여 설치 - 아래는 추가 내용 안내

**Airflow docker 와 생성한 디렉터리 내 항목들과 volume mount**
```bash
mkdir -p ./dags ./logs ./plugins ./config ./data
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
data 항목은 데이터 처리 과정중에 발생한 파일을 저장하기 위한 용도로 사용 합니다.
- 사용자 편의에 맞게 추가 설정 하면 됩니다.
- exchange_calendars 는 해외주식 거래일의 휴무 여부를 판별하기 위해 사용

docker-compose.yaml 수정
```bash
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
   
 environment:
  _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- exchange_calendars}
```

#### (3) Docker compose
Docker compose v2 부터는 `docker-compose` 대신 `docker compose` 로 실행


#### (4) Airflow  접속
Airflow 접속을 합니다. 포트 설정을 변경하지 않았다면 접속 주소는 아래와 같습니다.
- http://localhost:8080
- ID, PW 는 설정에 의존합니다.

(5) 이후 단계 부터는 본인의 사용 환경에 맞추어 구성합니다. 
이 리포에서는 S3 + Athena + AWS Glue 를 기반으로 데이터 저장 및 조회를 합니다.

#### 기타 - Grafana 설치
Grafana Open Source 버전으로 설치합니다.
```bash
docker run -d -p 3000:3000 --name=grafana-oss grafana/grafana-oss
```
Athena 를 사용할 예정이라면, BI 툴 내에서 plug-in 설치


### 3. 코드 안내
작성 중, 코드 업데이트 예정

