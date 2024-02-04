Get Stock Price Data with Airflow
===

Yahoo Finance API 와 Airflow를 이용하여 주가 데이터를 가져 옵니다.
- `2024-02-01` 기준 총 1562개 종목을 가져오고 있으며, 지수에 속한 종목을 가져 옵니다.
- 수집 목록 : S&P500, Nasdaq100, Dow30, S&P400, S&P600, etc.
- Russell 2000 은 종목 정보를 일관적으로 가져오기 어려운 부분이 있어서 S&P400, S&P600 으로 대체
- Analytics Engineering 역량을 키우기 위해 작업 하고 있습니다.


## 1. 설치 및 환경 구성
기본적으로 Docker 기반으로 설치를 하고 운영 합니다.
- Docker 설치는 Docker [공식 페이지](https://www.docker.com/)에서 확인
- Airflow 는 Docker 를 설치 후 [공식 페이지](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)에서 안내한 순서대로 설치

## 2. 안내 사항
**Airflow docker 와 생성한 디렉터리 내 항목들과 volume mount**
```bash
mkdir -p ./dags ./logs ./plugins ./config ./data
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
상세 내용
- data 디렉터리 하위에는 각 dag 별로 처리 할 때 생성하는 파일을 잠시 보관하기 위한 디렉터리가 있습니다.
- 각종 변수는 Airflow Variable 을 이용하고 관리 하므로 조회하는 리포에는 존재하지 않습니다.
- S3에 데이터를 저장하고 Athena + Glue 를 이용하여 데이터를 관리 및 조회 합니다.
- BI 는 metabase 와 Grafana 를 사용
- exchange_calendars 는 해외주식 거래일의 휴무 여부를 판별하기 위해 사용
- backfill 은 volume mount 를 하지 않습니다.

로컬 환경에서 작업을 할 때는 경로 설정을 해줘야 합니다. Airflow 에서는 다양한 방법을 안내 하고 있는데 아래의 방법으로 처리 
```bash
WORKSPACE_FOLDER='your airflow WORKSPACE_FOLDER'
PYTHONPATH=${WORKSPACE_FOLDER}/plugins
```

## 3. 코드 안내
** `2024-02-01` 이후 한국투자증권 API 는 사용하지 않습니다. 버그가 많이 있으며, 마스터 파일 관리가 되고 있지 않습니다. 현재는 미국 주식 데이터만 조회 하고 있습니다.
- us_market_stock_daily_price_yf.py : 일별로 데이터를 가져와 저장합니다. 1993-01-01을 기준(SPY 상장 시즌)으로 합니다.
- us_market_stock_daily_price_yf_ticker.py : 티커 기준으로 데이터를 가져와서 저장 합니다. 각 종목 별로 가져올 수 있는 최대 기간 데이터를 가져옵니다.
- us_market_stock_mdd.py : 각 종목별로 MDD 를 구합니다. [pyfolio](https://github.com/stefan-jansen/pyfolio-reloaded) 를 이용합니다.
- backfill 용 코드는 수정 중 입니다.


## 4. TODO
기본 목록
- [x] airflow dags 를 비롯한 전체적인 코드 구조 변경
- [x] S3 Partition 구조 변경 - 코드 변경
- [x] config 와 market open status 분리
- [x] Slack notification 개선
- [x] 에러 처리 코드 및 retry 기능 추가(airflow 기능)
- [x] 파일 관리 시스템 개선 - 진행 중
- [x] 코드 리팩토링 - Airflow **kwargs 분리, Pipeline 간소화
- [x] S3 버킷 이름 Airflow Variable 로 관리
- [x] us market 종목 중 지수 내 속하지 않은 항목, ETF 에 대해 파일로 관리 - 일부 작업 중
- [x] 각 종목별 배당 내역 : Annual Total Return  
- [x] Quant : 상대강도지수 구현
- [ ] Airflow 코드 리뷰 및 구조 변경

추가 목록
- [ ] Edgar API : https://www.sec.gov/edgar/sec-api-documentation
- [ ] 10-K, 10-Q, IR report, earning call with Gen AI

BI
- [x] Metabase BI 작업
- [ ] Grafana BI 작업

Dev
- [ ] AWS Cloud
- [ ] Devops - CI/CD

Investing
- [ ] Research

### 6. Sample Image

Sample Image - Metabase BI

| Sample Image 1                                                    | Sample Image 2                                                    |
|-------------------------------------------------------------------|-------------------------------------------------------------------|
| ![Alt text](img/Metabase%20Sample%20Image/1.png "optional title") | ![Alt text](img/Metabase%20Sample%20Image/2.png "optional title") |
| ![Alt text](img/Metabase%20Sample%20Image/3.png "optional title") | ![Alt text](img/Metabase%20Sample%20Image/4.png "optional title") |


| Market Index - Sample Image                                                    | AAPL - Sample Image                                                                    |
|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| ![Alt text](img/Metabase%20Sample%20Image/5_Market_Index.png "optional title") | ![Alt text](img/Metabase%20Sample%20Image/6_us_market_AAPL_History.png "optional title") |
