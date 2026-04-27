from collections.abc import Iterator
from pathlib import Path

from sqlmodel import Session, SQLModel, create_engine

from app.config import settings

if settings.database_url.startswith("sqlite:///"):
    db_path = Path(settings.database_url.removeprefix("sqlite:///"))
    db_path.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    settings.database_url,
    connect_args={"check_same_thread": False} if "sqlite" in settings.database_url else {},
)


def init_db() -> None:
    SQLModel.metadata.create_all(engine)
    _ensure_columns(
        "pstrykprice",
        [("kwh_import", "REAL"), ("kwh_export", "REAL"), ("cost_pln", "REAL")],
    )


def _ensure_columns(table: str, columns: list[tuple[str, str]]) -> None:
    """Add columns that aren't already on the SQLite table.

    SQLModel.create_all only handles fresh tables; existing tables
    keep their original column set unless we add them ourselves.
    Idempotent — safe to call on every startup.
    """
    if "sqlite" not in str(engine.url):
        return
    with engine.begin() as conn:
        existing = {row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table})")}
        for name, col_type in columns:
            if name not in existing:
                conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")


def get_session() -> Iterator[Session]:
    with Session(engine) as session:
        yield session
