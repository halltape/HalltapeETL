from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


class S3MaxDateManager:
    def __init__(
        self,
        table_name: str,
        init_date: str,
        postgres_conn_id: str = "metadata_db",
    ):
        self.table_name = table_name
        self.init_date = init_date
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def get_max_date(self) -> str:
        sql = """
            SELECT COALESCE(MAX(max_date), %s)
            FROM s3_max_dates
            WHERE table_name = %s
        """
        result = self.hook.get_first(sql, parameters=(self.init_date, self.table_name))
        return result[0].strftime("%Y-%m-%d")

    def update_max_date(self, date_value: str, updated_at=datetime.now()):
        sql = """
            INSERT INTO s3_max_dates (table_name, max_date, updated_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE
            SET max_date = EXCLUDED.max_date,
                updated_at = EXCLUDED.updated_at
        """
        self.hook.run(sql, parameters=(self.table_name, date_value, updated_at))
