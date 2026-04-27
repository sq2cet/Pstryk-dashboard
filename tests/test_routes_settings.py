from fastapi.testclient import TestClient
from sqlmodel import Session

from app.db import engine
from app.main import app
from app.services import settings_service as svc


def _session() -> Session:
    return Session(engine)


def test_get_settings_first_run_shows_warning() -> None:
    with TestClient(app) as client:
        response = client.get("/settings")
    assert response.status_code == 200
    assert "Configuration incomplete" in response.text
    assert "Pstryk Dashboard" in response.text


def test_post_settings_saves_and_redirects() -> None:
    with TestClient(app, follow_redirects=False) as client:
        response = client.post(
            "/settings",
            data={
                "pstryk_api_key": "real-pstryk-key",
                "blebox_host": "192.168.1.50",
                "blebox_port": "80",
                "pstryk_poll_minutes": "60",
                "blebox_live_seconds": "5",
                "blebox_persist_seconds": "60",
                "tz": "Europe/Warsaw",
            },
        )
    assert response.status_code == 303
    assert response.headers["location"] == "/settings?saved=1"

    with _session() as s:
        assert svc.get_plaintext(s, svc.PSTRYK_API_KEY) == "real-pstryk-key"
        assert svc.get_value(s, svc.BLEBOX_HOST) == "192.168.1.50"
        assert svc.get_view(s).is_configured() is True


def test_post_settings_blank_api_key_keeps_existing() -> None:
    with _session() as s:
        svc.set_value(s, svc.PSTRYK_API_KEY, "original-key")

    with TestClient(app, follow_redirects=False) as client:
        client.post(
            "/settings",
            data={
                "pstryk_api_key": "",  # blank — must NOT overwrite
                "blebox_host": "192.168.1.50",
            },
        )

    with _session() as s:
        assert svc.get_plaintext(s, svc.PSTRYK_API_KEY) == "original-key"
        assert svc.get_value(s, svc.BLEBOX_HOST) == "192.168.1.50"


def test_post_settings_ignores_non_numeric_intervals() -> None:
    with TestClient(app, follow_redirects=False) as client:
        client.post(
            "/settings",
            data={
                "pstryk_api_key": "k",
                "blebox_host": "x",
                "pstryk_poll_minutes": "abc",
                "blebox_live_seconds": "",
                "blebox_persist_seconds": "999",
            },
        )
    with _session() as s:
        view = svc.get_view(s)
        assert view.pstryk_poll_minutes == 60  # default kept (input rejected)
        assert view.blebox_live_seconds == 5  # default kept
        assert view.blebox_persist_seconds == 999  # numeric input accepted


def test_index_redirects_when_unconfigured() -> None:
    with TestClient(app, follow_redirects=False) as client:
        response = client.get("/")
    assert response.status_code == 303
    assert response.headers["location"] == "/settings"


def test_index_renders_dashboard_when_configured() -> None:
    with _session() as s:
        svc.set_value(s, svc.PSTRYK_API_KEY, "k")
        svc.set_value(s, svc.BLEBOX_HOST, "1.2.3.4")

    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "Pstryk Dashboard" in response.text
