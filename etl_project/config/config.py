from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


DatabaseType = Literal["sqlite", "mysql"]


@dataclass(slots=True)
class DatabaseSettings:
    db_type: DatabaseType = "sqlite"
    sqlite_path: Path = Path("etl_project/runtime/source.db")
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "etl_benchmark"


@dataclass(slots=True)
class ProjectSettings:
    project_root: Path = Path("etl_project").resolve()
    runtime_dir: Path = Path("etl_project/runtime").resolve()
    data_dir: Path = Path("etl_project/runtime/data").resolve()
    datasets_dir: Path = Path("etl_project/runtime/datasets").resolve()
    destinations_dir: Path = Path("etl_project/runtime/destinations").resolve()
    chunks_dir: Path = Path("etl_project/runtime/chunks").resolve()
    log_dir: Path = Path("etl_project/runtime/logs").resolve()
    results_dir: Path = Path("etl_project/results").resolve()
    source_db: DatabaseSettings = field(
        default_factory=lambda: DatabaseSettings(
            db_type="sqlite",
            sqlite_path=Path("etl_project/runtime/source.db").resolve(),
            database="source_benchmark",
        )
    )
    destination_db: DatabaseSettings = field(
        default_factory=lambda: DatabaseSettings(
            db_type="sqlite",
            sqlite_path=Path("etl_project/runtime/destination.db").resolve(),
            database="destination_benchmark",
        )
    )
    csv_export_path: Path = Path("etl_project/runtime/data/extracted_data.csv").resolve()
    json_export_path: Path = Path("etl_project/runtime/data/extracted_data.json").resolve()
    results_csv_path: Path = Path("etl_project/results/results.csv").resolve()
    plot_path: Path = Path("etl_project/results/benchmark_plot.png").resolve()

    def ensure_directories(self) -> None:
        for path in (
            self.runtime_dir,
            self.data_dir,
            self.datasets_dir,
            self.destinations_dir,
            self.chunks_dir,
            self.log_dir,
            self.results_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


settings = ProjectSettings()
