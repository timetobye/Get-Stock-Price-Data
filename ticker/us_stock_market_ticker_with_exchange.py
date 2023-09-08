import pandas as pd
import urllib.request
import zipfile
import os
import datetime

base_dir = os.getcwd()


def download_stock_index_zip_file(directory_path):
    download_zip_file_url = f"https://new.real.download.dws.co.kr/common/master/frgn_code.mst.zip"
    download_zip_file_path = f"{directory_path}{os.sep}frgn_code.mst.zip"
    urllib.request.urlretrieve(download_zip_file_url, download_zip_file_path)

    with zipfile.ZipFile(download_zip_file_path) as overseas_zip:
        with overseas_zip.open("frgn_code.mst") as f:
            content = f.read().decode("cp949")
            with open(f"{directory_path}{os.sep}frgn_code.mst", mode="w", encoding="cp949") as mst_file:
                mst_file.write(content)

    os.remove(download_zip_file_path)  # 필요시 활성화


def get_stock_index_data(directory_path):
    """
    작업의 편의상 한국투자증권에서 제공한 코드와 주석을 크게 변경하지 않음
    https://github.com/koreainvestment/open-trading-api/blob/main/stocks_info/overseas_index_code.py
    """
    now = datetime.datetime.now()
    date_str_utf8 = now.strftime("%Y%m%d").encode("utf-8").decode("utf-8")

    # df1 : '구분코드','심볼','영문명','한글명'
    mst_file_path = f"{directory_path}{os.sep}frgn_code.mst"
    tmp_part_one_file = f"{directory_path}{os.sep}frgn_code_part1.tmp"
    tmp_part_two_file = f"{directory_path}{os.sep}frgn_code_part2.tmp"

    tmp_part_one = open(tmp_part_one_file, mode="w")
    tmp_part_two = open(tmp_part_two_file, mode="w")

    with open(mst_file_path, mode="r", encoding="cp949") as f:
        for row in f:
            if row[0:1] == 'X':
                rf1 = row[0:len(row) - 14]
                rf_1 = rf1[0:1]
                rf1_2 = rf1[1:11]
                rf1_3 = rf1[11:40].replace(",", "")
                rf1_4 = rf1[40:80].replace(",", "").strip()
                tmp_part_one.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + ',' + rf1_4 + '\n')
                rf2 = row[-15:]
                tmp_part_two.write(rf2 + '\n')
                continue

            rf1 = row[0:len(row) - 14]
            rf1_1 = rf1[0:1]
            rf1_2 = rf1[1:11]
            rf1_3 = rf1[11:50].replace(",", "")
            rf1_4 = row[50:75].replace(",", "").strip()
            tmp_part_one.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + ',' + rf1_4 + '\n')
            rf2 = row[-15:]
            tmp_part_two.write(rf2 + '\n')

    tmp_part_one.close()
    tmp_part_two.close()

    part_one_columns = ['구분코드', '심볼', '영문명', '한글명']
    df1 = pd.read_csv(tmp_part_one_file, header=None, names=part_one_columns, encoding='utf-8')

    # df2 : '종목업종코드','다우30 편입종목여부','나스닥100 편입종목여부', 'S&P 500 편입종목여부','거래소코드','국가구분코드'

    field_specs = [4, 1, 1, 1, 4, 3]
    part_two_columns = ['종목업종코드', '다우30 편입종목여부', '나스닥100 편입종목여부',
                        'S&P 500 편입종목여부', '거래소코드', '국가구분코드']
    df2 = pd.read_fwf(tmp_part_two_file, widths=field_specs, names=part_two_columns, encoding='utf-8')

    df2['종목업종코드'] = df2['종목업종코드'].str.replace(
        pat=r'[^A-Z]', repl=r'', regex=True
    )  # 종목업종코드는 잘못 기입되어 있을 수 있으니 참고할 때 반드시 mst 파일과 비교 참고

    df2['다우30 편입종목여부'] = df2['다우30 편입종목여부'].str.replace(
        pat=r'[^0-1]+', repl=r'', regex=True
    )  # 한글명 길이가 길어서 생긴 오타들 제거

    df2['나스닥100 편입종목여부'] = df2['나스닥100 편입종목여부'].str.replace(
        pat=r'[^0-1]+', repl=r'', regex=True
    )

    df2['S&P 500 편입종목여부'] = df2['S&P 500 편입종목여부'].str.replace(
        pat=r'[^0-1]+', repl=r'', regex=True
    )

    # DF : df1 + df2
    concat_df = pd.concat([df1, df2], axis=1)
    excel_file_name = f"frgn_code_{date_str_utf8}.xlsx"
    concat_df.to_excel(excel_file_name, index=False)  # 현재 위치에 엑셀파일로 저장

    return concat_df


def main():
    current_directory_path = os.getcwd()
    download_stock_index_zip_file(current_directory_path)
    get_stock_index_data(current_directory_path)


if __name__ == "__main__":
    main()