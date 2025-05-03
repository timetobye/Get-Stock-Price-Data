import os
from pathlib import Path

from airflow import settings


class PathUtils:
    asset_dir = "assets"
    sql_dir = "sql"

    def __init__(self, current_path=__file__) -> None:
        self.root_path = Path(settings.DAGS_FOLDER)
        self.current_path = Path(current_path)
        self.parent_path = self.current_path.parent

    def get_sql_file_path(self, sql_file_name, market_region="us"):
        sql_file_path = os.path.join(
            self.root_path, market_region, self.asset_dir, self.sql_dir, sql_file_name
        )

        return sql_file_path
