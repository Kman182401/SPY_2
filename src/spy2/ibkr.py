from __future__ import annotations

import socket


def check_connectivity(
    *,
    host: str,
    port: int,
    timeout: float,
    confirm_read_only_unchecked: bool,
    client_name: str,
    expected_port: int,
) -> int:
    if not confirm_read_only_unchecked:
        raise SystemExit(
            "Read-Only must be unchecked in IBKR API settings. "
            "Re-run with --confirm-read-only-unchecked once verified."
        )

    if port != expected_port:
        raise SystemExit(
            f"{client_name} paper default port is {expected_port}. "
            f"Refusing to use {port} without explicit correction."
        )

    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        raise SystemExit(f"Unable to connect to {host}:{port}: {exc}") from exc

    print(f"Connected to {client_name} on {host}:{port}.")
    return 0
