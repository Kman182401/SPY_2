import datetime as dt
import json

import pandas as pd

from spy2.cli import main as cli_main
from spy2.options.chain import iter_chain_snapshots


def _write_parquet(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _build_sample_root(tmp_path) -> dt.date:
    trade_date = dt.date(2026, 2, 2)
    date_str = trade_date.isoformat()
    base = tmp_path / "data" / "raw"

    underlying_ts = pd.Timestamp("2026-02-02T14:30:00Z")
    underlying = pd.DataFrame(
        {
            "ts_event": [underlying_ts],
            "open": [399.5],
            "high": [401.0],
            "low": [399.0],
            "close": [400.0],
            "volume": [123_456],
            "symbol": ["SPY"],
        }
    )
    _write_parquet(
        underlying,
        base / "EQUS.MINI" / "ohlcv-1m" / f"date={date_str}" / "part-0000.parquet",
    )

    expiration = pd.Timestamp("2026-02-19T00:00:00Z")
    symbols = ["SPY   260219C00400000", "SPY   260219C00401000"]
    definitions = pd.DataFrame(
        {
            "symbol": symbols,
            "underlying": ["SPY", "SPY"],
            "strike_price": [400.0, 401.0],
            "expiration": [expiration, expiration],
        }
    )
    _write_parquet(
        definitions,
        base / "OPRA.PILLAR" / "definition" / f"date={date_str}" / "part-0000.parquet",
    )

    quote_ts = pd.Timestamp("2026-02-02T14:30:20Z")
    quotes = pd.DataFrame(
        {
            "ts_event": [quote_ts, quote_ts],
            "symbol": symbols,
            "bid_px_00": [1.00, 0.60],
            "ask_px_00": [1.10, 0.70],
            "bid_sz_00": [10, 15],
            "ask_sz_00": [12, 20],
        }
    )
    _write_parquet(
        quotes,
        base / "OPRA.PILLAR" / "cbbo-1m" / f"date={date_str}" / "part-0000.parquet",
    )

    # Minimal OPRA statistics rows for liquidity gating (volume + open interest).
    stats_ts = pd.Timestamp("2026-02-02T20:59:00Z")
    statistics = pd.DataFrame(
        {
            "ts_event": [stats_ts, stats_ts, stats_ts, stats_ts],
            "symbol": [symbols[0], symbols[0], symbols[1], symbols[1]],
            # Databento StatType codes: 6=CLEARED_VOLUME, 9=OPEN_INTEREST.
            "stat_type": [6, 9, 6, 9],
            "quantity": [1_000, 10_000, 1_000, 10_000],
        }
    )
    _write_parquet(
        statistics,
        base / "OPRA.PILLAR" / "statistics" / f"date={date_str}" / "part-0000.parquet",
    )
    return trade_date


def test_validate_day_cli_writes_report(tmp_path, capsys):
    trade_date = _build_sample_root(tmp_path)
    rc = cli_main.main(
        [
            "data",
            "validate-day",
            trade_date.isoformat(),
            "--root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    report = (
        tmp_path
        / "artifacts"
        / "validation"
        / f"validate_{trade_date.isoformat()}.json"
    )
    assert report.exists()
    payload = json.loads(report.read_text())
    assert payload["date"] == trade_date.isoformat()
    out = capsys.readouterr().out
    assert "Wrote" in out


def test_snapshots_head_cli(tmp_path, capsys):
    trade_date = _build_sample_root(tmp_path)
    rc = cli_main.main(
        [
            "snapshots",
            "head",
            trade_date.isoformat(),
            "--n",
            "1",
            "--root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "underlying=400.00" in out


def test_backtest_demo_cli(tmp_path, capsys):
    trade_date = _build_sample_root(tmp_path)
    rc = cli_main.main(
        [
            "backtest",
            "demo",
            trade_date.isoformat(),
            "--time",
            "14:30",
            "--root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "net_debit_dollars" in out


def test_merge_asof_alignment(tmp_path):
    trade_date = _build_sample_root(tmp_path)
    snapshots = list(
        iter_chain_snapshots(
            trade_date,
            root=tmp_path,
            quotes_schema="cbbo-1m",
            asof_tolerance_seconds=60,
        )
    )
    assert snapshots
    assert snapshots[0].underlying_price == 400.0


def test_backtest_run_cli_writes_artifacts(tmp_path, capsys):
    trade_date = _build_sample_root(tmp_path)
    rc = cli_main.main(
        [
            "backtest",
            "run",
            "--start",
            trade_date.isoformat(),
            "--end",
            trade_date.isoformat(),
            "--root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "summary.json" in out

    bt_root = tmp_path / "artifacts" / "backtests"
    assert bt_root.exists()
    run_dirs = [path for path in bt_root.iterdir() if path.is_dir()]
    assert run_dirs
    run_dir = run_dirs[0]
    assert (run_dir / "trades.parquet").exists()
    assert (run_dir / "equity_curve.parquet").exists()
    assert (run_dir / "summary.json").exists()


def test_ibkr_check_defaults_host_from_env(monkeypatch):
    monkeypatch.setenv("IBKR_HOST", "host.docker.internal")
    captured: dict[str, object] = {}

    def _fake_check_connectivity(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(cli_main.ibkr, "check_connectivity", _fake_check_connectivity)
    rc = cli_main.main(
        [
            "ibkr",
            "check",
            "--gateway",
            "--confirm-read-only-unchecked",
        ]
    )
    assert rc == 0
    assert captured["host"] == "host.docker.internal"
    assert captured["port"] == 4002
    assert captured["target_env"] == "paper"
