import os
import tempfile

import pytest
from cryptography.fernet import Fernet

os.environ.setdefault("FERNET_KEY", Fernet.generate_key().decode())
os.environ["PSTRYK_DISABLE_SCHEDULER"] = "1"

# Use a temp file so the SQLite DB survives across the multiple connections
# a single TestClient request may open. Each test starts with empty tables.
_fd, _path = tempfile.mkstemp(suffix=".db")
os.close(_fd)
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_path}")


@pytest.fixture(autouse=True)
def _reset_db():
    from sqlmodel import SQLModel

    from app.db import engine

    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    yield
