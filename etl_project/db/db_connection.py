from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Iterable

from etl_project.config.config import DatabaseSettings

try:
    import mysql.connector  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    mysql = None
else:  # pragma: no cover - exercised only when MySQL is installed
    mysql = mysql.connector


class DatabaseError(RuntimeError):
    """Raised when an unsupported or unavailable database backend is used."""


class DatabaseConnection:
    def __init__(self, settings: DatabaseSettings) -> None:
        self.settings = settings

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        if self.settings.db_type == "sqlite":
            with self._connect_sqlite() as connection:
                yield connection
            return

        if self.settings.db_type == "mysql":
            with self._connect_mysql() as connection:
                yield connection
            return

        raise DatabaseError(f"Unsupported database type: {self.settings.db_type}")

    @contextmanager
    def _connect_sqlite(self) -> Generator[sqlite3.Connection, None, None]:
        sqlite_path = Path(self.settings.sqlite_path)
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(sqlite_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    @contextmanager
    def _connect_mysql(self) -> Generator[Any, None, None]:
        if mysql is None:
            raise DatabaseError(
                "mysql-connector-python is required when db_type='mysql'."
            )

        connection = mysql.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            database=self.settings.database,
        )
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def execute_script(self, script: str) -> None:
        with self.connect() as connection:
            if self.settings.db_type == "sqlite":
                connection.executescript(script)
            else:
                cursor = connection.cursor()
                try:
                    for statement in self._split_sql_statements(script):
                        if statement:
                            cursor.execute(statement)
                finally:
                    cursor.close()

    def execute(
        self,
        query: str,
        params: Iterable[Any] | None = None,
        many: bool = False,
    ) -> None:
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                parameters = tuple(params or ())
                if many:
                    cursor.executemany(query, parameters)
                else:
                    cursor.execute(query, parameters)
            finally:
                cursor.close()

    def fetch_all(
        self, query: str, params: Iterable[Any] | None = None
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(query, tuple(params or ()))
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
            finally:
                cursor.close()
        return [dict(zip(columns, row)) for row in rows]

    def fetch_one(
        self, query: str, params: Iterable[Any] | None = None
    ) -> dict[str, Any] | None:
        results = self.fetch_all(query, params)
        return results[0] if results else None

    @staticmethod
    def _split_sql_statements(script: str) -> list[str]:
        return [statement.strip() for statement in script.split(";") if statement.strip()]
