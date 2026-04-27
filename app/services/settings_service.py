"""Persisted user settings — read/write helpers with at-rest encryption.

Settings live as rows in the `Setting` table keyed by name. Values flagged
`is_secret` are stored Fernet-encrypted; callers that need plaintext use
`get_secret()` which decrypts on read.

`pstryk_api_key` is the only secret today; new ones can be declared by
adding a key to `_SECRET_KEYS`.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlmodel import Session, select

from app.crypto import decrypt, encrypt
from app.models import Setting, utcnow_naive

PSTRYK_API_KEY = "pstryk_api_key"
BLEBOX_HOST = "blebox_host"
BLEBOX_PORT = "blebox_port"
PSTRYK_POLL_MINUTES = "pstryk_poll_minutes"
BLEBOX_LIVE_SECONDS = "blebox_live_seconds"
BLEBOX_PERSIST_SECONDS = "blebox_persist_seconds"
TIMEZONE = "tz"

_SECRET_KEYS = frozenset({PSTRYK_API_KEY})

DEFAULTS: dict[str, str] = {
    BLEBOX_PORT: "80",
    PSTRYK_POLL_MINUTES: "60",
    BLEBOX_LIVE_SECONDS: "5",
    BLEBOX_PERSIST_SECONDS: "60",
    TIMEZONE: "Europe/Warsaw",
}


@dataclass(frozen=True)
class SettingsView:
    """A read-only snapshot used by templates and callers.

    The Pstryk API key is never returned in plaintext from this view —
    only a boolean `pstryk_api_key_set` flag — so accidental logging
    or template rendering cannot leak it.
    """

    pstryk_api_key_set: bool
    blebox_host: str | None
    blebox_port: int
    pstryk_poll_minutes: int
    blebox_live_seconds: int
    blebox_persist_seconds: int
    tz: str

    def is_configured(self) -> bool:
        return self.pstryk_api_key_set and bool(self.blebox_host)


def get_value(session: Session, key: str) -> str | None:
    """Return the raw stored value (still encrypted, if secret)."""
    row = session.get(Setting, key)
    return row.value if row else None


def get_plaintext(session: Session, key: str) -> str | None:
    """Return the plaintext value, decrypting if the key is a secret."""
    row = session.get(Setting, key)
    if row is None:
        return None
    if row.is_secret:
        return decrypt(row.value)
    return row.value


def set_value(session: Session, key: str, value: str) -> None:
    """Upsert a setting. Encrypts if the key is in `_SECRET_KEYS`."""
    is_secret = key in _SECRET_KEYS
    stored = encrypt(value) if is_secret else value
    row = session.get(Setting, key)
    if row is None:
        row = Setting(key=key, value=stored, is_secret=is_secret, updated_at=utcnow_naive())
        session.add(row)
    else:
        row.value = stored
        row.is_secret = is_secret
        row.updated_at = utcnow_naive()
        session.add(row)
    session.commit()


def delete(session: Session, key: str) -> None:
    row = session.get(Setting, key)
    if row is not None:
        session.delete(row)
        session.commit()


def _int_with_default(session: Session, key: str) -> int:
    raw = get_value(session, key)
    if raw is None:
        raw = DEFAULTS[key]
    try:
        return int(raw)
    except ValueError:
        return int(DEFAULTS[key])


def get_view(session: Session) -> SettingsView:
    return SettingsView(
        pstryk_api_key_set=session.get(Setting, PSTRYK_API_KEY) is not None,
        blebox_host=get_value(session, BLEBOX_HOST),
        blebox_port=_int_with_default(session, BLEBOX_PORT),
        pstryk_poll_minutes=_int_with_default(session, PSTRYK_POLL_MINUTES),
        blebox_live_seconds=_int_with_default(session, BLEBOX_LIVE_SECONDS),
        blebox_persist_seconds=_int_with_default(session, BLEBOX_PERSIST_SECONDS),
        tz=get_value(session, TIMEZONE) or DEFAULTS[TIMEZONE],
    )


def all_keys_present(session: Session) -> list[str]:
    rows = session.exec(select(Setting.key)).all()
    return list(rows)
