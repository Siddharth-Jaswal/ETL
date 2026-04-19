from __future__ import annotations

from pathlib import Path

from etl_project.config.config import settings


def delete_file_if_exists(file_path: Path) -> None:
    if file_path.exists() and file_path.is_file():
        file_path.unlink()


def delete_directory_contents(directory_path: Path) -> None:
    if not directory_path.exists():
        return

    for path in directory_path.iterdir():
        if path.is_dir():
            delete_directory_contents(path)
            path.rmdir()
        else:
            path.unlink()


def cleanup_runtime_temporary_artifacts() -> None:
    settings.ensure_directories()
    delete_directory_contents(settings.destinations_dir)
    delete_directory_contents(settings.chunks_dir)

    for csv_file in settings.data_dir.glob("*.csv"):
        csv_file.unlink()
