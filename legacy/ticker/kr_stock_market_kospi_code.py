"""
이 코드는 한국투자증권 KIS Developers 에서 게시한 코드를 일부 수정한 것 입니다.(OS 및 인코딩 이슈 등)
자세한 내용은 F&Q 에서 제목 : 종목정보 다운로드(국내) 를 참고해주세요.
코스피 정보를 가져옵니다.
https://github.com/koreainvestment/open-trading-api/blob/main/stocks_info/kis_kospi_code_mst.py

종목코드는 ['ST', 'EF', 'RT', 'IF'] 항목만 선택 하도록 수정하였습니다.
- ST : 주권
- EF : ETF
- RT : 리츠
- IF : 사회간접투자자본(맥쿼리)
"""

import pandas as pd
import urllib.request
import zipfile
import os
import datetime


def download_kospi_stock_code_file(directory_path):
    download_zip_file_url = f"https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
    download_zip_file_path = f"{directory_path}{os.sep}kospi_code.zip"
    urllib.request.urlretrieve(download_zip_file_url, download_zip_file_path)


def unzip_kospi_stock_code_zip_file(directory_path):
    zip_file_path = f"{directory_path}{os.sep}kospi_code.zip"
    overseas_zip = zipfile.ZipFile(zip_file_path)
    overseas_zip.extractall()
    overseas_zip.close()
    os.remove(zip_file_path)


def get_kospi_master_dataframe(directory_path):
    mst_file_path = f"{directory_path}{os.sep}kospi_code.mst"
    tmp_file1 = f"{directory_path}{os.sep}kospi_code_part1.tmp"
    tmp_file2 = f"{directory_path}{os.sep}kospi_code_part2.tmp"

    wf1 = open(tmp_file1, mode="w")
    wf2 = open(tmp_file2, mode="w")

    with open(mst_file_path, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 228]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-228:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드', '표준코드', '한글종목명']
    df1 = pd.read_csv(tmp_file1, header=None, names=part1_columns, encoding='utf-8')

    field_specs = [2, 1, 4, 4, 4,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 1, 1, 1, 1,
                   1, 9, 5, 5, 1,
                   1, 1, 2, 1, 1,
                   1, 2, 2, 2, 3,
                   1, 3, 12, 12, 8,
                   15, 21, 2, 7, 1,
                   1, 1, 1, 1, 9,
                   9, 9, 5, 9, 8,
                   9, 3, 1, 1, 1
                   ]

    part2_columns = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
                     '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
                     'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
                     'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
                     'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
                     'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
                     'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
                     '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
                     '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
                     '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
                     '상장주수', '자본금', '결산월', '공모가', '우선주',
                     '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
                     '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
                     '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
                     ]

    df2 = pd.read_fwf(tmp_file2, widths=field_specs, names=part2_columns)

    df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)
    # ref : https://wikidocs.net/178118
    group_code_selection_list = ['ST', 'EF', 'RT', 'IF']
    df = df[df['그룹코드'].isin(group_code_selection_list)]

    # clean temporary file and dataframe
    del (df1)
    del (df2)
    os.remove(mst_file_path)
    os.remove(tmp_file1)
    os.remove(tmp_file2)

    now = datetime.datetime.now()
    date_str = now.strftime("%Y%m%d")

    df.to_excel(f'kospi_code_{date_str}.xlsx', index=False)  # 현재 위치에 엑셀파일로 저장
    print("Done")


def main():
    base_dir = os.getcwd()

    download_kospi_stock_code_file(base_dir)
    unzip_kospi_stock_code_zip_file(base_dir)
    get_kospi_master_dataframe(base_dir)


if __name__ == "__main__":
    main()
