import pandas as pd
import urllib.request
import zipfile
import os
import datetime


def download_stock_code_file(directory_path, val):
    # ssl._create_default_https_context = ssl._create_unverified_context()
    download_zip_file_url = f"https://new.real.download.dws.co.kr/common/master/{val}mst.cod.zip"
    download_zip_file_path = f"{directory_path}{os.sep}{val}mst.cod.zip"
    urllib.request.urlretrieve(download_zip_file_url, download_zip_file_path)


def unzip_stock_code_zip_file(directory_path, val):
    zip_file_path = f"{directory_path}{os.sep}{val}mst.cod.zip"
    overseas_zip = zipfile.ZipFile(zip_file_path)
    overseas_zip.extractall()
    overseas_zip.close()
    os.remove(zip_file_path)
    # os.chdir(current_directory_path)


def get_stock_exchange_data_df(directory_path, val):
    cod_file_path = f"{directory_path}{os.sep}{val}mst.cod"
    columns = [
        'National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol',
        'Korea name', 'English name', 'Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)', 'currency',
        'float position', 'data type', 'base price', 'Bid order size', 'Ask order size',
        'market start time(HHMM)', 'market end time(HHMM)', 'DR 여부(Y/N)', 'DR 국가코드', '업종분류코드',
        '지수구성종목 존재 여부(0:구성종목없음,1:구성종목있음)', 'Tick size Type',
        '구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)',
        'Tick size type 상세'
    ]
    print(f"Downloading : {val}mst.cod file")
    stock_exchange_data_df = pd.read_table(cod_file_path, sep='\t', encoding='cp949')
    stock_exchange_data_df.columns = columns
    os.remove(cod_file_path)  # remove cod file

    # 필요할 경우 활성화
    # excel_file_name = f'{val}_code.xlsx'
    # stock_data_df.to_excel(excel_file_name, index=False)  # 현재 위치에 엑셀파일로 저장

    return stock_exchange_data_df


def main():
    """
    # 나스닥, 뉴욕, 아멕스 : 'nas', 'nys', 'ams'
    # 상해, 상해지수, 심천, 심천지수, 도쿄, 홍콩, 하노이, 호치민 : 'shs', 'shi', 'szs', 'szi', 'tse', 'hks', 'hnx', 'hsx'
    """
    current_directory_path = os.getcwd()
    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")

    stock_exchange_name_list = ['nas', 'nys', 'ams']  # 순서 대로 나스닥, 뉴욕, 아멕스

    all_stock_exchange_df = pd.DataFrame()
    for index, stock_exchange_name in enumerate(stock_exchange_name_list):
        download_stock_code_file(current_directory_path, stock_exchange_name)
        unzip_stock_code_zip_file(current_directory_path, stock_exchange_name)
        stock_exchange_data_df = get_stock_exchange_data_df(current_directory_path, stock_exchange_name)
        all_stock_exchange_df = pd.concat([all_stock_exchange_df, stock_exchange_data_df], axis=0)

    print(f"Concat overseas_stock_code_all_{date_str}.xlsx")
    all_stock_exchange_df.to_excel(f"overseas_stock_code_all_{date_str}.xlsx", index=False)  # 전체 통합 파일
    print("Done")


if __name__ == "__main__":
    main()

'''
이 코드는 한국투자증권 KIS Developers 에서 게시한 코드를 일부 수정한 것 입니다.
자세한 내용은 F&Q 에서 제목 : 종목정보 다운로드(해외) 를 참고해주세요.
https://github.com/koreainvestment/open-trading-api/blob/main/stocks_info/overseas_stock_code.py

해외주식종목코드 정제 파이썬 파일
미국 : nasmst.cod, nysmst.cod, amsmst.cod,
중국 : shsmst.cod, shimst.cod, szsmst.cod, szimst.cod,
일본 : tsemst.cod,
홍콩 : hksmst.cod,
베트남 : hnxmst.cod, hsxmst.cod'''

'''
※ 유의사항 ※
실행 환경 혹은 원본 파일의 칼럼 수의 변경으로 간혹 정제코드 파일(overseas_stock_code.py)이 실행되지 않을 수 있습니다.
해당 경우, URL에 아래 링크를 복사+붙여넣기 하여 원본 파일을 다운로드하시기 바랍니다.
. https://new.real.download.dws.co.kr/common/master/{val}mst.cod.zip
. {val} 자리에 원하시는 시장코드를 넣어주세요.
. 'nas','nys','ams','shs','shi','szs','szi','tse','hks','hnx','hsx'
. 순서대로 나스닥, 뉴욕, 아멕스, 상해, 상해지수, 심천, 심천지수, 도쿄, 홍콩, 하노이, 호치민

https://new.real.download.dws.co.kr/common/master/nasmst.cod.zip
'''
