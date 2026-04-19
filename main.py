from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from etl_project.config.config import settings
from etl_project.data_generator.generate_data import (
    build_sqlite_dataset_settings,
    generate_benchmark_datasets,
)
from etl_project.etl.case1_direct import run_case1_direct
from etl_project.etl.case2_file import run_case2_file
from etl_project.etl.case3_parallel import get_common_chunk_sizes, run_case3_parallel
from etl_project.results.plot_results import render_results_plot, render_results_table
from etl_project.utils.chunk_utils import get_file_size_mb
from etl_project.utils.runtime_cleanup import cleanup_runtime_temporary_artifacts


DATASET_RECORDS = [200_000, 400_000, 600_000, 800_000, 1_000_000]


@dataclass(frozen=True, slots=True)
class BenchmarkRow:
    SrNo: int
    dataset_name: str
    records: int
    records_label: str
    db_size_mb: float
    case1_sec: float
    case2_sec: float
    case3_a_chunk_mb: int
    case3_a_sec: float
    case3_b_chunk_mb: int
    case3_b_sec: float
    case3_c_chunk_mb: int
    case3_c_sec: float
    case3_d_chunk_mb: int
    case3_d_sec: float
    case3_optimal_sec: float
    case3_optimal_chunk_mb: int

    def to_csv_row(self) -> dict[str, object]:
        return {
            "SrNo": self.SrNo,
            "dataset_name": self.dataset_name,
            "#Records": self.records_label,
            "RECORD_COUNT": self.records,
            "DB_SIZE_MB": round(self.db_size_mb, 4),
            "CASE1_SEC": round(self.case1_sec, 4),
            "CASE2_SEC": round(self.case2_sec, 4),
            f"{self.case3_a_chunk_mb}_MB_SEC": round(self.case3_a_sec, 4),
            f"{self.case3_b_chunk_mb}_MB_SEC": round(self.case3_b_sec, 4),
            f"{self.case3_c_chunk_mb}_MB_SEC": round(self.case3_c_sec, 4),
            f"{self.case3_d_chunk_mb}_MB_SEC": round(self.case3_d_sec, 4),
            "CASE3_OPTIMAL_SEC": round(self.case3_optimal_sec, 4),
            "CASE3_OPTIMAL_CHUNK_MB": self.case3_optimal_chunk_mb,
        }


def ensure_datasets_exist() -> list[dict[str, object]]:
    missing = []
    for record_count in DATASET_RECORDS:
        dataset_name = f"source_{record_count}"
        dataset_settings = build_sqlite_dataset_settings(dataset_name)
        if not dataset_settings.sqlite_path.exists():
            missing.append(record_count)

    if missing:
        return generate_benchmark_datasets(missing, reset=True, batch_size=20_000)
    return []


def build_results_rows() -> list[BenchmarkRow]:
    settings.ensure_directories()
    ensure_datasets_exist()

    dataset_names = [f"source_{record_count}" for record_count in DATASET_RECORDS]
    common_chunk_sizes = get_common_chunk_sizes(dataset_names)
    if len(common_chunk_sizes) != 4:
        raise RuntimeError(f"Expected 4 shared chunk sizes, got {common_chunk_sizes}")

    rows: list[BenchmarkRow] = []
    for index, record_count in enumerate(DATASET_RECORDS, start=1):
        dataset_name = f"source_{record_count}"
        dataset_path = build_sqlite_dataset_settings(dataset_name).sqlite_path
        db_size_mb = get_file_size_mb(dataset_path)

        case1_result = run_case1_direct(dataset_name, cleanup_destination=True)
        case2_result = run_case2_file(
            dataset_name,
            cleanup_destination=True,
            cleanup_files=True,
        )
        case3_results = {
            chunk_size: run_case3_parallel(
                dataset_name,
                chunk_size,
                cleanup_destination=True,
            )
            for chunk_size in common_chunk_sizes
        }

        optimal_case3 = min(case3_results.values(), key=lambda result: result.elapsed_seconds)
        rows.append(
            BenchmarkRow(
                SrNo=index,
                dataset_name=dataset_name,
                records=record_count,
                records_label=f"{record_count // 100_000}L",
                db_size_mb=db_size_mb,
                case1_sec=case1_result.elapsed_seconds,
                case2_sec=case2_result.elapsed_seconds,
                case3_a_chunk_mb=common_chunk_sizes[0],
                case3_a_sec=case3_results[common_chunk_sizes[0]].elapsed_seconds,
                case3_b_chunk_mb=common_chunk_sizes[1],
                case3_b_sec=case3_results[common_chunk_sizes[1]].elapsed_seconds,
                case3_c_chunk_mb=common_chunk_sizes[2],
                case3_c_sec=case3_results[common_chunk_sizes[2]].elapsed_seconds,
                case3_d_chunk_mb=common_chunk_sizes[3],
                case3_d_sec=case3_results[common_chunk_sizes[3]].elapsed_seconds,
                case3_optimal_sec=optimal_case3.elapsed_seconds,
                case3_optimal_chunk_mb=optimal_case3.chunk_size_mb,
            )
        )

    return rows


def write_results_csv(rows: list[BenchmarkRow], output_path: Path | None = None) -> Path:
    output_path = (output_path or settings.results_csv_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    csv_rows = [row.to_csv_row() for row in rows]

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(csv_rows)

    return output_path


def run_benchmark() -> dict[str, str]:
    cleanup_runtime_temporary_artifacts()
    rows = build_results_rows()
    results_csv = write_results_csv(rows)
    results_table = render_results_table(results_csv)
    results_plot = render_results_plot(results_csv)
    cleanup_runtime_temporary_artifacts()
    return {
        "results_csv": str(results_csv),
        "results_table_png": str(results_table),
        "results_plot_png": str(results_plot),
    }


if __name__ == "__main__":
    print(run_benchmark())
