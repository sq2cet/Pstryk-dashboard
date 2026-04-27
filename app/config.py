from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    fernet_key: str
    database_url: str = f"sqlite:///{PROJECT_ROOT / 'data' / 'pstryk.db'}"
    tz: str = "Europe/Warsaw"
    host: str = "127.0.0.1"
    port: int = 8000


settings = Settings()  # type: ignore[call-arg]
