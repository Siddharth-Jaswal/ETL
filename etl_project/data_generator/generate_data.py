from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle, islice
from pathlib import Path
from typing import Iterable

from etl_project.config.config import DatabaseSettings, settings
from etl_project.db.create_db import DatabaseInitializer
from etl_project.db.db_connection import DatabaseConnection


@dataclass(frozen=True, slots=True)
class BaseRecord:
    name: str
    roll_no: str
    email: str
    phone_number: str


class DataGenerator:
    def __init__(self, database_settings: DatabaseSettings | None = None) -> None:
        self.database_settings = database_settings or settings.source_db
        self.connection = DatabaseConnection(self.database_settings)
        self.initializer = DatabaseInitializer()
        self.base_records: tuple[BaseRecord, ...] = (
            BaseRecord("Aarav Sharma", "1001", "aarav@example.com", "9876543210"),
            BaseRecord("Diya Patel", "1002", "diya@example.com", "9123456780"),
            BaseRecord("Ishaan Verma", "1003", "ishaan@example.com", "9988776655"),
            BaseRecord("Meera Iyer", "1004", "meera@example.com", "9090909090"),
            BaseRecord("Rohan Gupta", "1005", "rohan@example.com", "9812345678"),
        )

    def reset_source_database(self) -> None:
        settings.ensure_directories()
        self.initializer.initialize(self.database_settings, reset=True)

    def generate(self, record_count: int, reset: bool = True, batch_size: int = 10_000) -> None:
        if record_count < 0:
            raise ValueError("record_count must be non-negative")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        if reset:
            self.reset_source_database()

        insert_query = """
            INSERT INTO records (name, roll_no, email, phone_number)
            VALUES (?, ?, ?, ?)
        """

        generated = 0
        while generated < record_count:
            current_batch_size = min(batch_size, record_count - generated)
            batch = self._build_batch(start_index=generated, batch_size=current_batch_size)
            self.connection.execute(insert_query, params=batch, many=True)
            generated += current_batch_size

    def get_record_count(self) -> int:
        result = self.connection.fetch_one("SELECT COUNT(*) AS total FROM records")
        return int(result["total"]) if result else 0

    def get_database_size_mb(self) -> float:
        sqlite_path = self.database_settings.sqlite_path
        if self.database_settings.db_type != "sqlite":
            raise NotImplementedError("DB size calculation is implemented for SQLite only.")
        if not sqlite_path.exists():
            return 0.0
        return round(sqlite_path.stat().st_size / (1024 * 1024), 4)

    def summarize(self) -> dict[str, float | int]:
        return {
            "record_count": self.get_record_count(),
            "db_size_mb": self.get_database_size_mb(),
        }

    def _build_batch(self, start_index: int, batch_size: int) -> list[tuple[str, str, str, str]]:
        records = islice(cycle(self.base_records), start_index, start_index + batch_size)
        return [self._materialize_record(base_record, start_index + offset) for offset, base_record in enumerate(records)]

    @staticmethod
    def _materialize_record(
        base_record: BaseRecord, sequence_number: int
    ) -> tuple[str, str, str, str]:
        unique_roll = f"{base_record.roll_no}_{sequence_number + 1}"
        local_part, domain = base_record.email.split("@", maxsplit=1)
        unique_email = f"{local_part}+{sequence_number + 1}@{domain}"
        return (
            base_record.name,
            unique_roll,
            unique_email,
            base_record.phone_number,
        )


def generate_source_data(record_count: int, reset: bool = True) -> dict[str, float | int]:
    generator = DataGenerator()
    generator.generate(record_count=record_count, reset=reset)
    return generator.summarize()


def build_sqlite_dataset_settings(dataset_name: str) -> DatabaseSettings:
    return DatabaseSettings(
        db_type="sqlite",
        sqlite_path=(settings.datasets_dir / f"{dataset_name}.db").resolve(),
        database=dataset_name,
    )


def generate_dataset_database(
    record_count: int,
    dataset_name: str,
    reset: bool = True,
    batch_size: int = 10_000,
) -> dict[str, float | int | str]:
    dataset_settings = build_sqlite_dataset_settings(dataset_name)
    generator = DataGenerator(database_settings=dataset_settings)
    generator.generate(record_count=record_count, reset=reset, batch_size=batch_size)
    summary = generator.summarize()
    summary["dataset_name"] = dataset_name
    summary["database_path"] = str(dataset_settings.sqlite_path)
    return summary


def generate_benchmark_datasets(
    record_counts: Iterable[int],
    reset: bool = True,
    batch_size: int = 10_000,
) -> list[dict[str, float | int | str]]:
    summaries: list[dict[str, float | int | str]] = []
    for record_count in record_counts:
        dataset_name = f"source_{record_count}"
        summaries.append(
            generate_dataset_database(
                record_count=record_count,
                dataset_name=dataset_name,
                reset=reset,
                batch_size=batch_size,
            )
        )
    return summaries


if __name__ == "__main__":
    benchmark_sizes = [200_000, 400_000, 600_000, 800_000, 1_000_000]
    summaries = generate_benchmark_datasets(record_counts=benchmark_sizes, reset=True)
    for summary in summaries:
        print(summary)
