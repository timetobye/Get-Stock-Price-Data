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
mkdir -p ./dags ./logs ./plugins ./config ./data ./backfill
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
data 항목은 데이터 처리 과정중에 발생한 파일을 저장하기 위한 용도로 사용 합니다.
- 사용자 편의에 맞게 추가 설정 하면 됩니다.
- exchange_calendars 는 해외주식 거래일의 휴무 여부를 판별하기 위해 사용
- backfill 은 volumn mount 를 하지 않습니다.

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

#### (5) Config 파일 설정
ini 파일로 작성 하였습니다.

```bash
cd config
touch config.ini
```

```ini
# config.ini

[LIVE_APP]
key = None
secret_key = None 
access_token = None
hash = None

[TEST_APP]
key = NONE
secret_key = NONE
access_token = NONE
hash = NONE

[AWS_CONFIG]
aws_access_key = None
aws_secret_access_key = None

[SLACK_CONFIG]
channel_access_token = None
channel_name =None
```

(5) 이후 단계 부터는 본인의 사용 환경에 맞추어 구성합니다. 
이 리포에서는 S3 + Athena + AWS Glue 를 기반으로 데이터 저장 및 조회를 합니다.

#### 기타 - Grafana 설치
Grafana Open Source 버전으로 설치합니다.
```bash
docker run -d -p 3000:3000 --name=grafana-oss grafana/grafana-oss
```
Athena 를 사용할 예정이라면, BI 툴 내에서 plug-in 설치


### 3. 코드 안내

#### dags

Airflow 에서 사용할 dags 목록 입니다.
- config 를 제외한 마켓 데이터를 가져오는 dag 는 한국 시간으로 오후 5 ~ 7시 사이에 실행됩니다.
- 그렇게 실행하는 이유는 한투에서 제공(연합인포맥스 제공)하는 마스터 파일의 갱신 시간이 5 ~ 6시 입니다.
- 기타 사항으로 데이터가 없는 경우도 존재하여, 수동으로 채워 넣어야 하는데 이 부분은 향후 업데이트 됩니다.

config_generate.py
- 매일 config 정보를 갱신하는 dag 입니다.
- 토큰 등은 24시간의 만료 기간이 있기 때문에 사용을 하려면 매일 갱신을 해줘야 합니다.
- 현재 설정된 시간은 KST 02:00(24시 기준) 입니다.
- 마켓의 Open Status 도 체크를 하는데 향후 분리될 부분입니다.

kr_market
- kr_market_stock_code.py : 지정된 시간에 Kospi, Kosdaq 정보를 가져옵니다. 모든 정보를 가져오지는 않고, KRX300 및 일정 시총을 넘는 항목을 가져옵니다.
- kr_market_stock_daily_price.py : 가져온 정보를 바탕으로 당일 가격을 가져옵니다. 약간의 시간이 소요됩니다. 
- kr_market_upload_csv_data_to_s3.py : 가격 데이터를 S3 로 저장합니다.

us_market
- us_market_stock_code_with_index.py : S&P500, DOW30, NASDAQ100 에 속한 종목과 기타 종목 몇 가지에 대한 정보를 가져옵니다.
  - 각 지수 정보는 Wikipedia 에서 가져옵니다. 종목의 편입, 편출 정보가 잘 관리되고 있으며, 내용 추적에 용이 합니다.
  - [S&P500](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies), [DOW30](https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average#Components), [NASDAQ100](https://en.wikipedia.org/wiki/Nasdaq-100#Components)
  - 각 지수 정보에 속하지 않은 종목(주로 ETF - SPY, DIA, QQQ 등)도 가져옵니다. 다만 이 경우는 하드코딩 되어 있고, 향후 파일로 관리하는 방향으로 처리 될 예정입니다.
- us_market_stock_daily_price.py : 가져온 지수 내 종목 정보에 해당하는 가격 정보를 가져옵니다.
  - 특정 종목의 경우 한투에서 데이터 제공을 하지 않아 누락되는 경우가 있습니다. 주로 특정 시점 이후로 데이터를 제공하는 경우에 해당합니다.
- us_market_upload_csv_data_to_s3.py : 가격 데이터를 S3 로 저장합니다.
- Ticker : A 의 경우 한투에서 제공하는 마스터파일에서 누락이 된 상태라 세부 정보가 없고, 가격 정보만 존재합니다.


#### backfill

airflow 오류 등으로 인해 데이터 누락이 발생할 경우, backfill 하기 위해 작성한 코드 입니다.
- day, month 로 나누어져 있습니다.
- day 의 경우 세부 날짜(YYYYMMDD) 를 지정할 수 있습니다.
- month의 경우 년월(YYYYMM)까지 지정을 하면, 해당 기간에 맞는 날짜를 계산하여 데이터를 가져옵니다.
- API 이용이 1초에 20회를 넘길 경우 connection 이 종료될 수 있으므로, time.sleep이 적용되어 있습니다.

```bash
# day
python3 backfill_kr_market_stock_daily_price_day.py --start_date_str 20230901 --end_date_str 20230902

# month
python3 backfill_kr_market_stock_daily_price_month.py --start_year_and_month 202309 --end_year_and_month 202309
```

### 4. 기타 안내
테이블 스키마에 대한 정보는 API 문서와 수집한 데이터를 바탕으로 직접 구성하거나, S3 + [AWS Glue](https://aws.amazon.com/ko/glue/) 를 이용하여 구성할 수 있습니다.
S3 를 사용할 경우 버킷 이름을 지정해주셔야 합니다.

작업 환경 구성은 Pycharm Prof version + Custom Airflow Docker file 을 연결하여 구성하였습니다.
- https://www.jetbrains.com/help/pycharm/docker-images.html#push-image
- custom docker file

```bash
## pull or check docker image
docker pull apache/airflow:2.6.3
```

```bash
touch Dockerfile

# Airflow + ABC 라이브러리 설치
FROM apache/airflow:2.7.1  
RUN pip install 'ABC'
```

```bash
# build dockerfile
docker build -t my-custom-airflow:2.6.3 .
```

```bash
# run custom airflow
docker run -d my-custom-airflow:2.7.1
```

### 5. TODO

공통
- [ ] config 와 market open status 분리
- [ ] Airflow Subdags 기능을 이용하여 kr, us market dag 묶기
- [ ] Slack notification 개선
- [ ] 에러 처리 코드 및 retry 기능 추가(airflow 기능)
- [ ] 파일 관리 시스템 개선
- [ ] 코드 리팩토링 - Airflow **kwargs 분리, Pipeline 간소화
- [ ] S3 버킷 이름 config 로 관리

KR
- [ ] 각 종목별 배당 내역 : https://www.data.go.kr/data/15001153/openapi.do

US
- [ ] us market 종목 중 지수 내 속하지 않은 항목, ETF 에 대해 파일로 관리
- [ ] 각 종목별 배당 내역 : Annual Total Return  
- [ ] Quant : RS

추가
- [ ] Edgar API : https://www.sec.gov/edgar/sec-api-documentation
- [ ] Dart API : https://opendart.fss.or.kr/intro/main.do

기타
- [ ] Grafana, Metabase BI 작업