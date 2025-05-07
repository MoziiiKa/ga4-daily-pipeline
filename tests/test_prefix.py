"""Unit test: date‑prefix path builder"""

from src.ingest.main import RAW_PREFIX, FILE_NAME, _build_target_path


def test_build_target_path(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    target = _build_target_path()
    assert target.startswith(f"{RAW_PREFIX}/")
    assert target.endswith(f"/{FILE_NAME}")
    # path contains 3 folders after RAW_PREFIX: YYYY/MM/DD
    assert len(target.split("/")) == 5
