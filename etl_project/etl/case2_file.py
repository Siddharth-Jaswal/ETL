from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from etl_project.config.config import DatabaseSettings, settings
from etl_project.db.create_db import DatabaseInitializer
from etl_project.db.db_connection import DatabaseConnection
from etl_project.etl.transformations import transform_record
from etl_project.utils.file_utils import ensure_parent_directory, iter_csv
from etl_project.utils.runtime_cleanup import delete_file_if_exists
from etl_project.utils.timer import timed
import csv


CSV_FIELDS = ["name", "roll_no", "email", "phone_number"]


@dataclass(frozen=True, slots=True)
class FileETLResult:
    source_database: str
    extracted_file: str
    transformed_file: str
    destination_database: str
    records_processed: int
    elapsed_seconds: float


class FileBasedETL:
    def __init__(
        self,
        source_settings: DatabaseSettings,
        destination_settings: DatabaseSettings,
        extracted_file_path: Path,
        transformed_file_path: Path,
    ) -> None:
        self.source_settings = source_settings
        self.destination_settings = destination_settings
        self.extracted_file_path = extracted_file_path
        self.transformed_file_path = transformed_file_path
        self.source_connection = DatabaseConnection(source_settings)
        self.destination_connection = DatabaseConnection(destination_settings)
        self.initializer = DatabaseInitializer()

    def prepare_destination(self, reset: bool = True) -> None:
        settings.ensure_directories()
        self.initializer.initialize(self.destination_settings, reset=reset)

    @timed
    def run(self, reset_destination: bool = True) -> dict[str, Any]:
        self.prepare_destination(reset=reset_destination)
        extracted_count = self.extract_to_csv()
        transformed_count = self.transform_csv()
        loaded_count = self.load_transformed_csv()

        if extracted_count != transformed_count or transformed_count != loaded_count:
            raise RuntimeError("Case 2 ETL counts do not match across extract/transform/load stages.")

        return {
            "source_database": str(self.source_settings.sqlite_path),
            "extracted_file": str(self.extracted_file_path),
            "transformed_file": str(self.transformed_file_path),
            "destination_database": str(self.destination_settings.sqlite_path),
            "records_processed": loaded_count,
        }

    def extract_to_csv(self) -> int:
        ensure_parent_directory(self.extracted_file_path)
        count = 0
        select_query = "SELECT name, roll_no, email, phone_number FROM records ORDER BY id"
        with self.source_connection.connect() as source_conn, self.extracted_file_path.open(
            "w", newline="", encoding="utf-8"
        ) as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
            writer.writeheader()
            cursor = source_conn.cursor()
            try:
                cursor.execute(select_query)
                while True:
                    row = cursor.fetchone()
                    if row is None:
                        break
                    writer.writerow(dict(row))
                    count += 1
            finally:
                cursor.close()
        return count

    def transform_csv(self) -> int:
        ensure_parent_directory(self.transformed_file_path)
        count = 0
        with self.transformed_file_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for row in iter_csv(self.extracted_file_path):
                writer.writerow(dict(zip(CSV_FIELDS, transform_record(row))))
                count += 1
        return count

    def load_transformed_csv(self, batch_size: int = 10_000) -> int:
        insert_query = """
            INSERT INTO records (name, roll_no, email, phone_number)
            VALUES (?, ?, ?, ?)
        """
        records_loaded = 0
        batch: list[tuple[str, str, str, str]] = []
        with self.destination_connection.connect() as destination_conn:
            cursor = destination_conn.cursor()
            try:
                for row in iter_csv(self.transformed_file_path):
                    batch.append(
                        (row["name"], row["roll_no"], row["email"], row["phone_number"])
                    )
                    if len(batch) >= batch_size:
                        cursor.executemany(insert_query, batch)
                        records_loaded += len(batch)
                        batch.clear()

                if batch:
                    cursor.executemany(insert_query, batch)
                    records_loaded += len(batch)
            finally:
                cursor.close()
        return records_loaded


def build_case2_destination_settings(source_database_name: str) -> DatabaseSettings:
    destination_name = f"case2_{source_database_name}"
    return DatabaseSettings(
        db_type="sqlite",
        sqlite_path=(settings.destinations_dir / f"{destination_name}.db").resolve(),
        database=destination_name,
    )


def build_case2_file_paths(source_database_name: str) -> tuple[Path, Path]:
    extracted_path = settings.data_dir / f"{source_database_name}_extracted.csv"
    transformed_path = settings.data_dir / f"{source_database_name}_transformed.csv"
    return extracted_path.resolve(), transformed_path.resolve()


def run_case2_file(
    source_database_name: str,
    cleanup_destination: bool = False,
    cleanup_files: bool = False,
) -> FileETLResult:
    source_settings = DatabaseSettings(
        db_type="sqlite",
        sqlite_path=(settings.datasets_dir / f"{source_database_name}.db").resolve(),
        database=source_database_name,
    )
    destination_settings = build_case2_destination_settings(source_database_name)
    extracted_file_path, transformed_file_path = build_case2_file_paths(source_database_name)

    etl = FileBasedETL(
        source_settings=source_settings,
        destination_settings=destination_settings,
        extracted_file_path=extracted_file_path,
        transformed_file_path=transformed_file_path,
    )
    payload, elapsed_seconds = etl.run(reset_destination=True)
    result = FileETLResult(
        source_database=str(payload["source_database"]),
        extracted_file=str(payload["extracted_file"]),
        transformed_file=str(payload["transformed_file"]),
        destination_database=str(payload["destination_database"]),
        records_processed=int(payload["records_processed"]),
        elapsed_seconds=elapsed_seconds,
    )
    if cleanup_destination:
        delete_file_if_exists(destination_settings.sqlite_path)
    if cleanup_files:
        delete_file_if_exists(extracted_file_path)
        delete_file_if_exists(transformed_file_path)
    return result


if __name__ == "__main__":
    result = run_case2_file("source_500000")
    print(result)
