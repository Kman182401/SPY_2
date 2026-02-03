import pytest

from spy2.cli import main as cli_main


def test_cli_help_exits_zero():
    with pytest.raises(SystemExit) as excinfo:
        cli_main.main(["--help"])
    assert excinfo.value.code == 0


def test_cli_missing_api_key_error(monkeypatch):
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        cli_main.main(["databento", "list-schemas", "EQUS.MINI"])
    assert "Missing Databento API key" in str(excinfo.value)
