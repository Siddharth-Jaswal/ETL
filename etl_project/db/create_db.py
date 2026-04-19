from __future__ import annotations

from pathlib import Path

from etl_project.config.config import DatabaseSettings, settings
from etl_project.db.db_connection import DatabaseConnection


class DatabaseInitializer:
    def __init__(self, schema_path: Path | None = None) -> None:
        self.schema_path = schema_path or settings.project_root / "db" / "schema.sql"

    def initialize(self, database_settings: DatabaseSettings, reset: bool = False) -> None:
        if reset and database_settings.db_type == "sqlite":
            sqlite_path = Path(database_settings.sqlite_path)
            if sqlite_path.exists():
                sqlite_path.unlink()

        schema_sql = self.schema_path.read_text(encoding="utf-8")
        DatabaseConnection(database_settings).execute_script(schema_sql)


def initialize_databases(reset: bool = False) -> None:
    settings.ensure_directories()
    initializer = DatabaseInitializer()
    initializer.initialize(settings.source_db, reset=reset)
    initializer.initialize(settings.destination_db, reset=reset)


if __name__ == "__main__":
    initialize_databases(reset=True)
