"""
이 코드는 한국투자증권 KIS Developers 에서 게시한 코드를 일부 수정한 것 입니다.(OS 및 인코딩 이슈 등)
자세한 내용은 F&Q 에서 제목 : 종목정보 다운로드(국내) 를 참고해주세요.
코스닥 정보를 가져옵니다.
https://github.com/koreainvestment/open-trading-api/blob/main/stocks_info/kis_kosdaq_code_mst.py

종목코드는 구분없이 제공하는 모든 항목을 선택합니다.
"""

import pandas as pd
import urllib.request
import zipfile
import os
import datetime


def download_kosdaq_stock_code_file(directory_path):
    download_zip_file_url = f"https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"
    download_zip_file_path = f"{directory_path}{os.sep}kosdaq_code.zip"
    urllib.request.urlretrieve(download_zip_file_url, download_zip_file_path)

def unzip_kosdaq_stock_code_zip_file(directory_path):
    zip_file_path = f"{directory_path}{os.sep}kosdaq_code.zip"
    overseas_zip = zipfile.ZipFile(zip_file_path)
    overseas_zip.extractall()
    overseas_zip.close()
    os.remove(zip_file_path)


def get_kospi_master_dataframe(directory_path):
    mst_file_path = f"{directory_path}{os.sep}kosdaq_code.mst"
    tmp_file1 = f"{directory_path}{os.sep}kosdaq_code_part1.tmp"
    tmp_file2 = f"{directory_path}{os.sep}kosdaq_code_part2.tmp"

    wf1 = open(tmp_file1, mode="w")
    wf2 = open(tmp_file2, mode="w")

    with open(mst_file_path, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 222]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-222:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드', '표준코드', '한글종목명']
    df1 = pd.read_csv(tmp_file1, header=None, names=part1_columns, encoding='utf-8')

    field_specs = [2, 1,
                   4, 4, 4, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 9,
                   5, 5, 1, 1, 1,
                   2, 1, 1, 1, 2,
                   2, 2, 3, 1, 3,
                   12, 12, 8, 15, 21,
                   2, 7, 1, 1, 1,
                   1, 9, 9, 9, 5,
                   9, 8, 9, 3, 1,
                   1, 1
                   ]

    part2_columns = ['증권그룹구분코드', '시가총액 규모 구분 코드 유가',
                     '지수업종 대분류 코드', '지수 업종 중분류 코드', '지수업종 소분류 코드', '벤처기업 여부 (Y/N)',
                     '저유동성종목 여부', 'KRX 종목 여부', 'ETP 상품구분코드', 'KRX100 종목 여부 (Y/N)',
                     'KRX 자동차 여부', 'KRX 반도체 여부', 'KRX 바이오 여부', 'KRX 은행 여부', '기업인수목적회사여부',
                     'KRX 에너지 화학 여부', 'KRX 철강 여부', '단기과열종목구분코드', 'KRX 미디어 통신 여부',
                     'KRX 건설 여부', '(코스닥)투자주의환기종목여부', 'KRX 증권 구분', 'KRX 선박 구분',
                     'KRX섹터지수 보험여부', 'KRX섹터지수 운송여부', 'KOSDAQ150지수여부 (Y,N)', '주식 기준가',
                     '정규 시장 매매 수량 단위', '시간외 시장 매매 수량 단위', '거래정지 여부', '정리매매 여부',
                     '관리 종목 여부', '시장 경고 구분 코드', '시장 경고위험 예고 여부', '불성실 공시 여부',
                     '우회 상장 여부', '락구분 코드', '액면가 변경 구분 코드', '증자 구분 코드', '증거금 비율',
                     '신용주문 가능 여부', '신용기간', '전일 거래량', '주식 액면가', '주식 상장 일자', '상장 주수(천)',
                     '자본금', '결산 월', '공모 가격', '우선주 구분 코드', '공매도과열종목여부', '이상급등종목여부',
                     'KRX300 종목 여부 (Y/N)', '매출액', '영업이익', '경상이익', '단기순이익', 'ROE(자기자본이익률)',
                     '기준년월', '전일기준 시가총액 (억)', '그룹사 코드', '회사신용한도초과여부', '담보대출가능여부', '대주가능여부'
                     ]

    df2 = pd.read_fwf(tmp_file2, widths=field_specs, names=part2_columns)

    df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

    # clean temporary file and dataframe
    del (df1)
    del (df2)
    os.remove(mst_file_path)
    os.remove(tmp_file1)
    os.remove(tmp_file2)

    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")

    df.to_excel(f'kosdaq_code_{date_str}.xlsx', index=False)  # 현재 위치에 엑셀파일로 저장
    print("Done")


def main():
    base_dir = os.getcwd()

    download_kosdaq_stock_code_file(base_dir)
    unzip_kosdaq_stock_code_zip_file(base_dir)
    get_kospi_master_dataframe(base_dir)


if __name__ == "__main__":
    main()
