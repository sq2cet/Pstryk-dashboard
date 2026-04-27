import pytest
from sqlmodel import Session

from app.db import engine
from app.services import settings_service as svc


@pytest.fixture
def session():
    with Session(engine) as s:
        yield s


def test_set_and_get_plain_value(session: Session) -> None:
    svc.set_value(session, svc.BLEBOX_HOST, "192.168.1.50")
    assert svc.get_value(session, svc.BLEBOX_HOST) == "192.168.1.50"
    assert svc.get_plaintext(session, svc.BLEBOX_HOST) == "192.168.1.50"


def test_secret_is_encrypted_at_rest_but_decrypts_on_read(session: Session) -> None:
    svc.set_value(session, svc.PSTRYK_API_KEY, "super-secret-key")

    raw = svc.get_value(session, svc.PSTRYK_API_KEY)
    assert raw is not None
    assert raw != "super-secret-key"  # ciphertext, not plaintext

    plaintext = svc.get_plaintext(session, svc.PSTRYK_API_KEY)
    assert plaintext == "super-secret-key"


def test_get_view_reflects_configuration_state(session: Session) -> None:
    view = svc.get_view(session)
    assert view.pstryk_api_key_set is False
    assert view.blebox_host is None
    assert view.is_configured() is False
    assert view.blebox_port == 80  # default

    svc.set_value(session, svc.PSTRYK_API_KEY, "k")
    svc.set_value(session, svc.BLEBOX_HOST, "10.0.0.1")
    view = svc.get_view(session)

    assert view.pstryk_api_key_set is True
    assert view.blebox_host == "10.0.0.1"
    assert view.is_configured() is True


def test_set_value_overwrites_existing(session: Session) -> None:
    svc.set_value(session, svc.BLEBOX_HOST, "first")
    svc.set_value(session, svc.BLEBOX_HOST, "second")
    assert svc.get_value(session, svc.BLEBOX_HOST) == "second"


def test_get_view_falls_back_to_defaults_for_invalid_int(session: Session) -> None:
    svc.set_value(session, svc.PSTRYK_POLL_MINUTES, "not-a-number")
    view = svc.get_view(session)
    assert view.pstryk_poll_minutes == int(svc.DEFAULTS[svc.PSTRYK_POLL_MINUTES])


def test_delete_removes_setting(session: Session) -> None:
    svc.set_value(session, svc.BLEBOX_HOST, "10.0.0.1")
    svc.delete(session, svc.BLEBOX_HOST)
    assert svc.get_value(session, svc.BLEBOX_HOST) is None
