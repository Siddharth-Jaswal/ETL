from __future__ import annotations

from dataclasses import dataclass
from queue import Queue
from threading import Thread
from typing import Any

from etl_project.config.config import DatabaseSettings, settings
from etl_project.db.create_db import DatabaseInitializer
from etl_project.db.db_connection import DatabaseConnection
from etl_project.etl.transformations import transform_record
from etl_project.utils.chunk_utils import (
    determine_common_chunk_sizes_mb,
    determine_chunk_sizes_mb,
    estimate_record_size_bytes,
    get_file_size_mb,
)
from etl_project.utils.runtime_cleanup import delete_file_if_exists
from etl_project.utils.timer import timed


EXTRACT_SENTINEL = object()
TRANSFORM_SENTINEL = object()


@dataclass(frozen=True, slots=True)
class ParallelETLResult:
    source_database: str
    destination_database: str
    records_processed: int
    chunk_size_mb: int
    elapsed_seconds: float


class ParallelChunkETL:
    def __init__(
        self,
        source_settings: DatabaseSettings,
        destination_settings: DatabaseSettings,
        chunk_size_mb: int,
        queue_capacity: int = 4,
    ) -> None:
        self.source_settings = source_settings
        self.destination_settings = destination_settings
        self.chunk_size_mb = chunk_size_mb
        self.chunk_size_bytes = chunk_size_mb * 1024 * 1024
        self.source_connection = DatabaseConnection(source_settings)
        self.destination_connection = DatabaseConnection(destination_settings)
        self.initializer = DatabaseInitializer()
        self.extract_queue: Queue[object] = Queue(maxsize=queue_capacity)
        self.transform_queue: Queue[object] = Queue(maxsize=queue_capacity)
        self.records_processed = 0

    def prepare_destination(self, reset: bool = True) -> None:
        settings.ensure_directories()
        self.initializer.initialize(self.destination_settings, reset=reset)

    @timed
    def run(self, reset_destination: bool = True) -> dict[str, Any]:
        self.prepare_destination(reset=reset_destination)

        extractor = Thread(target=self._extract_worker, name="extract-worker")
        transformer = Thread(target=self._transform_worker, name="transform-worker")
        loader = Thread(target=self._load_worker, name="load-worker")

        extractor.start()
        transformer.start()
        loader.start()

        extractor.join()
        transformer.join()
        loader.join()

        return {
            "source_database": str(self.source_settings.sqlite_path),
            "destination_database": str(self.destination_settings.sqlite_path),
            "records_processed": self.records_processed,
            "chunk_size_mb": self.chunk_size_mb,
        }

    def _extract_worker(self) -> None:
        select_query = "SELECT name, roll_no, email, phone_number FROM records ORDER BY id"
        with self.source_connection.connect() as source_conn:
            cursor = source_conn.cursor()
            try:
                cursor.execute(select_query)
                current_chunk: list[dict[str, Any]] = []
                current_size = 0

                while True:
                    row = cursor.fetchone()
                    if row is None:
                        if current_chunk:
                            self.extract_queue.put(current_chunk)
                        break

                    record = dict(row)
                    record_size = estimate_record_size_bytes(record)
                    current_chunk.append(record)
                    current_size += record_size

                    if current_size >= self.chunk_size_bytes:
                        self.extract_queue.put(current_chunk)
                        current_chunk = []
                        current_size = 0
            finally:
                cursor.close()

        self.extract_queue.put(EXTRACT_SENTINEL)

    def _transform_worker(self) -> None:
        while True:
            chunk = self.extract_queue.get()
            if chunk is EXTRACT_SENTINEL:
                self.transform_queue.put(TRANSFORM_SENTINEL)
                self.extract_queue.task_done()
                break

            transformed_chunk = [
                transform_record(record)
                for record in chunk  # type: ignore[arg-type]
            ]
            self.transform_queue.put(transformed_chunk)
            self.extract_queue.task_done()

    def _load_worker(self) -> None:
        insert_query = """
            INSERT INTO records (name, roll_no, email, phone_number)
            VALUES (?, ?, ?, ?)
        """
        with self.destination_connection.connect() as destination_conn:
            cursor = destination_conn.cursor()
            try:
                while True:
                    chunk = self.transform_queue.get()
                    if chunk is TRANSFORM_SENTINEL:
                        self.transform_queue.task_done()
                        break

                    cursor.executemany(insert_query, chunk)  # type: ignore[arg-type]
                    self.records_processed += len(chunk)  # type: ignore[arg-type]
                    self.transform_queue.task_done()
            finally:
                cursor.close()


def build_case3_destination_settings(
    source_database_name: str, chunk_size_mb: int
) -> DatabaseSettings:
    destination_name = f"case3_{source_database_name}_{chunk_size_mb}mb"
    return DatabaseSettings(
        db_type="sqlite",
        sqlite_path=(settings.destinations_dir / f"{destination_name}.db").resolve(),
        database=destination_name,
    )


def build_source_settings(source_database_name: str) -> DatabaseSettings:
    return DatabaseSettings(
        db_type="sqlite",
        sqlite_path=(settings.datasets_dir / f"{source_database_name}.db").resolve(),
        database=source_database_name,
    )


def get_recommended_chunk_sizes(source_database_name: str) -> list[int]:
    source_settings = build_source_settings(source_database_name)
    db_size_mb = get_file_size_mb(source_settings.sqlite_path)
    return determine_chunk_sizes_mb(db_size_mb)


def get_common_chunk_sizes(source_database_names: list[str]) -> list[int]:
    db_sizes_mb = [
        get_file_size_mb(build_source_settings(dataset_name).sqlite_path)
        for dataset_name in source_database_names
    ]
    return determine_common_chunk_sizes_mb(db_sizes_mb)


def run_case3_parallel(
    source_database_name: str,
    chunk_size_mb: int,
    cleanup_destination: bool = False,
) -> ParallelETLResult:
    source_settings = build_source_settings(source_database_name)
    destination_settings = build_case3_destination_settings(
        source_database_name=source_database_name,
        chunk_size_mb=chunk_size_mb,
    )

    etl = ParallelChunkETL(
        source_settings=source_settings,
        destination_settings=destination_settings,
        chunk_size_mb=chunk_size_mb,
    )
    payload, elapsed_seconds = etl.run(reset_destination=True)
    result = ParallelETLResult(
        source_database=str(payload["source_database"]),
        destination_database=str(payload["destination_database"]),
        records_processed=int(payload["records_processed"]),
        chunk_size_mb=int(payload["chunk_size_mb"]),
        elapsed_seconds=elapsed_seconds,
    )
    if cleanup_destination:
        delete_file_if_exists(destination_settings.sqlite_path)
    return result


if __name__ == "__main__":
    dataset_name = "source_500000"
    chunk_sizes = get_recommended_chunk_sizes(dataset_name)
    print({"dataset": dataset_name, "chunk_sizes_mb": chunk_sizes})
    print(run_case3_parallel(dataset_name, chunk_size_mb=chunk_sizes[0]))
