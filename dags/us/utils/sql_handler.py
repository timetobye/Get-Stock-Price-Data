import os

from us.utils.path_utils import PathUtils


def read_sql_file(file_name):

    path_utils = PathUtils()
    sql_file_path = path_utils.get_sql_file_path(file_name)
    print(sql_file_path)

    """파일 경로에서 SQL 쿼리 읽기"""
    with open(sql_file_path, "r") as file:
        query = file.read()
    print(query)
    return query


def get_query_from_file(sql_file_name):
    """SQL 파일을 읽고 쿼리 반환"""
    return read_sql_file(sql_file_name)
