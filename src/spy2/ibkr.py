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
    valid_ports: set[int] | None = None,
    allow_nondefault_port: bool = False,
    target_env: str = "paper",
) -> int:
    if not confirm_read_only_unchecked:
        raise SystemExit(
            "Read-Only must be unchecked in IBKR API settings. "
            "Re-run with --confirm-read-only-unchecked once verified."
        )

    allowed_ports = set(valid_ports or {expected_port})
    if port not in allowed_ports:
        allowed_text = ", ".join(str(p) for p in sorted(allowed_ports))
        raise SystemExit(
            f"{client_name} supported ports are: {allowed_text}. "
            f"Refusing to use unsupported port {port}."
        )

    if port != expected_port and not allow_nondefault_port:
        raise SystemExit(
            f"{client_name} {target_env} default port is {expected_port}. "
            f"Refusing to use non-default port {port}. "
            "Re-run with --allow-nondefault-port once verified."
        )

    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        raise SystemExit(f"Unable to connect to {host}:{port}: {exc}") from exc

    print(f"Connected to {client_name} on {host}:{port} ({target_env}).")
    return 0
