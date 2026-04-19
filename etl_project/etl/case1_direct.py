from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from etl_project.config.config import DatabaseSettings, settings
from etl_project.db.create_db import DatabaseInitializer
from etl_project.db.db_connection import DatabaseConnection
from etl_project.etl.transformations import transform_record
from etl_project.utils.runtime_cleanup import delete_file_if_exists
from etl_project.utils.timer import timed


@dataclass(frozen=True, slots=True)
class DirectETLResult:
    source_database: str
    destination_database: str
    records_processed: int
    elapsed_seconds: float


class DirectETL:
    def __init__(
        self,
        source_settings: DatabaseSettings,
        destination_settings: DatabaseSettings,
    ) -> None:
        self.source_settings = source_settings
        self.destination_settings = destination_settings
        self.source_connection = DatabaseConnection(source_settings)
        self.destination_connection = DatabaseConnection(destination_settings)
        self.initializer = DatabaseInitializer()

    def prepare_destination(self, reset: bool = True) -> None:
        settings.ensure_directories()
        self.initializer.initialize(self.destination_settings, reset=reset)

    @timed
    def run(self, reset_destination: bool = True) -> dict[str, Any]:
        self.prepare_destination(reset=reset_destination)

        records_processed = 0
        select_query = "SELECT name, roll_no, email, phone_number FROM records ORDER BY id"
        insert_query = """
            INSERT INTO records (name, roll_no, email, phone_number)
            VALUES (?, ?, ?, ?)
        """

        with self.source_connection.connect() as source_conn, self.destination_connection.connect() as destination_conn:
            source_cursor = source_conn.cursor()
            destination_cursor = destination_conn.cursor()
            try:
                source_cursor.execute(select_query)
                while True:
                    row = source_cursor.fetchone()
                    if row is None:
                        break

                    record = dict(row)
                    destination_cursor.execute(insert_query, transform_record(record))
                    records_processed += 1
            finally:
                source_cursor.close()
                destination_cursor.close()

        return {
            "source_database": str(self.source_settings.sqlite_path),
            "destination_database": str(self.destination_settings.sqlite_path),
            "records_processed": records_processed,
        }


def build_case1_destination_settings(source_database_name: str) -> DatabaseSettings:
    destination_name = f"case1_{source_database_name}"
    return DatabaseSettings(
        db_type="sqlite",
        sqlite_path=(settings.destinations_dir / f"{destination_name}.db").resolve(),
        database=destination_name,
    )


def run_case1_direct(
    source_database_name: str, cleanup_destination: bool = False
) -> DirectETLResult:
    source_settings = DatabaseSettings(
        db_type="sqlite",
        sqlite_path=(settings.datasets_dir / f"{source_database_name}.db").resolve(),
        database=source_database_name,
    )
    destination_settings = build_case1_destination_settings(source_database_name)

    etl = DirectETL(source_settings=source_settings, destination_settings=destination_settings)
    payload, elapsed_seconds = etl.run(reset_destination=True)
    result = DirectETLResult(
        source_database=str(payload["source_database"]),
        destination_database=str(payload["destination_database"]),
        records_processed=int(payload["records_processed"]),
        elapsed_seconds=elapsed_seconds,
    )
    if cleanup_destination:
        delete_file_if_exists(destination_settings.sqlite_path)
    return result


if __name__ == "__main__":
    result = run_case1_direct("source_500000")
    print(result)
