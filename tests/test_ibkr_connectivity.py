from __future__ import annotations

import pytest

from spy2 import ibkr


class _DummySocket:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_check_connectivity_requires_read_only_unchecked():
    with pytest.raises(SystemExit, match="Read-Only must be unchecked"):
        ibkr.check_connectivity(
            host="127.0.0.1",
            port=4002,
            timeout=1.0,
            confirm_read_only_unchecked=False,
            client_name="IB Gateway",
            expected_port=4002,
        )


def test_check_connectivity_rejects_unsupported_port():
    with pytest.raises(SystemExit, match="supported ports are: 4001, 4002"):
        ibkr.check_connectivity(
            host="127.0.0.1",
            port=5555,
            timeout=1.0,
            confirm_read_only_unchecked=True,
            client_name="IB Gateway",
            expected_port=4002,
            valid_ports={4001, 4002},
        )


def test_check_connectivity_rejects_non_default_port_without_override():
    with pytest.raises(SystemExit, match="Refusing to use non-default port 4001"):
        ibkr.check_connectivity(
            host="127.0.0.1",
            port=4001,
            timeout=1.0,
            confirm_read_only_unchecked=True,
            client_name="IB Gateway",
            expected_port=4002,
            valid_ports={4001, 4002},
            allow_nondefault_port=False,
            target_env="paper",
        )


def test_check_connectivity_allows_non_default_port_with_override(monkeypatch, capsys):
    monkeypatch.setattr(
        ibkr.socket, "create_connection", lambda *_args, **_kwargs: _DummySocket()
    )
    rc = ibkr.check_connectivity(
        host="127.0.0.1",
        port=4001,
        timeout=1.0,
        confirm_read_only_unchecked=True,
        client_name="IB Gateway",
        expected_port=4002,
        valid_ports={4001, 4002},
        allow_nondefault_port=True,
        target_env="paper",
    )
    assert rc == 0
    assert (
        "Connected to IB Gateway on 127.0.0.1:4001 (paper)." in capsys.readouterr().out
    )
