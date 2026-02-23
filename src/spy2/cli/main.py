from __future__ import annotations

import argparse
import os
import sys
from importlib import metadata

from spy2 import ibkr


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spy2",
        description="Local SPY vertical spreads system.",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show the package version and exit.",
    )

    subparsers = parser.add_subparsers(dest="command")

    db_parser = subparsers.add_parser("databento", help="Databento utilities.")
    db_sub = db_parser.add_subparsers(dest="db_command")

    db_list = db_sub.add_parser(
        "list-schemas",
        help="List available schemas for a dataset.",
    )
    db_list.add_argument("dataset", help="Dataset ID, e.g. OPRA.PILLAR")
    db_list.add_argument(
        "--api-key",
        dest="api_key",
        help="Databento API key (falls back to DATABENTO_API_KEY).",
    )
    db_list.add_argument(
        "--root",
        default=None,
        help="Override repo/data root (default: auto-detect or SPY2_DATA_ROOT).",
    )
    db_list.set_defaults(func=_cmd_databento_list_schemas)

    db_ingest = db_sub.add_parser(
        "ingest",
        help="Ingest one day of SPY data (underlying + OPRA).",
    )
    db_ingest.add_argument(
        "date",
        help="Trading date in YYYY-MM-DD (UTC day boundaries).",
    )
    db_ingest.add_argument(
        "--api-key",
        dest="api_key",
        help="Databento API key (falls back to DATABENTO_API_KEY).",
    )
    db_ingest.add_argument(
        "--quotes-schema",
        choices=("cbbo-1m", "tcbbo"),
        default="cbbo-1m",
        help="OPRA quotes schema to request (default: cbbo-1m).",
    )
    db_ingest.add_argument(
        "--auto-clamp",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Clamp ingest window to dataset availability (default: true).",
    )
    db_ingest.add_argument(
        "--root",
        default=None,
        help="Override repo/data root (default: auto-detect or SPY2_DATA_ROOT).",
    )
    db_ingest.set_defaults(func=_cmd_databento_ingest)

    db_range = db_sub.add_parser(
        "ingest-range",
        help="Ingest a date range of SPY data (inclusive).",
    )
    db_range.add_argument("start_date", help="Start date YYYY-MM-DD.")
    db_range.add_argument("end_date", help="End date YYYY-MM-DD.")
    db_range.add_argument(
        "--api-key",
        dest="api_key",
        help="Databento API key (falls back to DATABENTO_API_KEY).",
    )
    db_range.add_argument(
        "--quotes-schema",
        choices=("cbbo-1m", "tcbbo"),
        default="cbbo-1m",
        help="OPRA quotes schema to request (default: cbbo-1m).",
    )
    db_range.add_argument(
        "--auto-clamp",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Clamp ingest window to dataset availability (default: true).",
    )
    db_range.add_argument(
        "--root",
        default=None,
        help="Override repo/data root (default: auto-detect or SPY2_DATA_ROOT).",
    )
    db_range.set_defaults(func=_cmd_databento_ingest_range)

    data_parser = subparsers.add_parser("data", help="Data utilities.")
    data_sub = data_parser.add_subparsers(dest="data_command")

    data_validate = data_sub.add_parser(
        "validate-day",
        help="Validate required data partitions for a day.",
    )
    data_validate.add_argument(
        "date",
        help="Trading date in YYYY-MM-DD.",
    )
    data_validate.add_argument(
        "--quotes-schema",
        choices=("cbbo-1m", "tcbbo"),
        default="cbbo-1m",
        help="OPRA quotes schema to validate (default: cbbo-1m).",
    )
    data_validate.add_argument(
        "--root",
        default=None,
        help="Override repo root (default: auto-detect).",
    )
    data_validate.set_defaults(func=_cmd_data_validate_day)

    snapshots_parser = subparsers.add_parser(
        "snapshots", help="Option chain snapshots."
    )
    snapshots_sub = snapshots_parser.add_subparsers(dest="snap_command")
    snapshots_head = snapshots_sub.add_parser(
        "head",
        help="Print the first N snapshots for a day.",
    )
    snapshots_head.add_argument(
        "date",
        help="Trading date in YYYY-MM-DD.",
    )
    snapshots_head.add_argument(
        "--n",
        type=int,
        default=3,
        help="Number of snapshots to print (default: 3).",
    )
    snapshots_head.add_argument(
        "--quotes-schema",
        choices=("cbbo-1m", "tcbbo"),
        default="cbbo-1m",
        help="OPRA quotes schema to use (default: cbbo-1m).",
    )
    snapshots_head.add_argument(
        "--root",
        default=None,
        help="Override repo root (default: auto-detect).",
    )
    snapshots_head.set_defaults(func=_cmd_snapshots_head)

    backtest_parser = subparsers.add_parser("backtest", help="Backtest utilities.")
    backtest_sub = backtest_parser.add_subparsers(dest="bt_command")
    backtest_demo = backtest_sub.add_parser(
        "demo",
        help="Run a single spread fill demo for a day.",
    )
    backtest_demo.add_argument(
        "date",
        help="Trading date in YYYY-MM-DD.",
    )
    backtest_demo.add_argument(
        "--time",
        dest="time_str",
        default=None,
        help="Target time in HH:MM (UTC). Uses first snapshot if omitted.",
    )
    backtest_demo.add_argument(
        "--right",
        choices=("C", "P"),
        default="C",
        help="Option right to use (default: C).",
    )
    backtest_demo.add_argument(
        "--width",
        type=float,
        default=1.0,
        help="Vertical spread width in strike units (default: 1.0).",
    )
    backtest_demo.add_argument(
        "--quotes-schema",
        choices=("cbbo-1m", "tcbbo"),
        default="cbbo-1m",
        help="OPRA quotes schema to use (default: cbbo-1m).",
    )
    backtest_demo.add_argument(
        "--slippage-bps",
        type=float,
        default=0.0,
        help="Slippage in basis points (default: 0).",
    )
    backtest_demo.add_argument(
        "--root",
        default=None,
        help="Override repo root (default: auto-detect).",
    )
    backtest_demo.set_defaults(func=_cmd_backtest_demo)

    backtest_run = backtest_sub.add_parser(
        "run",
        help="Run a multi-day portfolio backtest.",
    )
    backtest_run.add_argument(
        "--start",
        required=True,
        help="Start date YYYY-MM-DD (trading sessions only).",
    )
    backtest_run.add_argument(
        "--end",
        required=True,
        help="End date YYYY-MM-DD (trading sessions only).",
    )
    backtest_run.add_argument(
        "--strategy",
        default="demo_vertical",
        help="Strategy name (default: demo_vertical). Options: demo_vertical, baseline_otm_credit.",
    )
    backtest_run.add_argument(
        "--right",
        choices=("C", "P"),
        default="P",
        help="Option right to use (default: P).",
    )
    backtest_run.add_argument(
        "--width",
        type=float,
        default=1.0,
        help="Vertical spread width in strike units (default: 1.0).",
    )
    backtest_run.add_argument(
        "--structure",
        choices=("debit", "credit"),
        default="debit",
        help="Vertical structure to model (default: debit).",
    )
    backtest_run.add_argument(
        "--quotes-schema",
        choices=("cbbo-1m", "tcbbo"),
        default="cbbo-1m",
        help="OPRA quotes schema to use (default: cbbo-1m).",
    )
    backtest_run.add_argument(
        "--slippage-bps",
        type=float,
        default=0.0,
        help="Slippage in basis points (default: 0).",
    )
    backtest_run.add_argument(
        "--initial-cash",
        type=float,
        default=1000.0,
        help="Initial cash in USD (default: 1000).",
    )
    backtest_run.add_argument(
        "--calendar",
        default="XNYS",
        help="Exchange calendar code for sessions (default: XNYS).",
    )
    backtest_run.add_argument(
        "--force-close-dte",
        type=int,
        default=1,
        help="Force close positions when DTE <= N (default: 1).",
    )
    backtest_run.add_argument(
        "--sel-dte-min",
        type=int,
        default=21,
        help="Selection DTE min (used by baseline strategies) (default: 21).",
    )
    backtest_run.add_argument(
        "--sel-dte-max",
        type=int,
        default=45,
        help="Selection DTE max (used by baseline strategies) (default: 45).",
    )
    backtest_run.add_argument(
        "--sel-otm-pct",
        type=float,
        default=0.01,
        help="Selection OTM percent (used by baseline strategies) (default: 0.01).",
    )
    backtest_run.add_argument(
        "--sel-min-credit",
        type=float,
        default=0.20,
        help="Selection minimum credit (used by baseline credit strategy) (default: 0.20).",
    )
    backtest_run.add_argument(
        "--fill-model",
        choices=(
            "conservative",
            "mid",
            "mid_with_slippage",
            "spread_inside",
            "spread_inside_with_slippage",
        ),
        default="conservative",
        help="Fill model to use (default: conservative).",
    )
    backtest_run.add_argument(
        "--fill-alpha",
        type=float,
        default=0.5,
        help=(
            "Spread fill alpha for spread_inside models: 0=net mid, 1=net ask "
            "(default: 0.5)."
        ),
    )
    backtest_run.add_argument(
        "--fill-sensitivity",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Also run the backtest under alternate fill models and write deltas (default: false).",
    )
    backtest_run.add_argument(
        "--exit-profit-take-frac",
        type=float,
        default=None,
        help=(
            "Enable profit-take exit: close when unrealized PnL >= frac*max_profit "
            "(fallback: frac*abs(entry_cashflow))."
        ),
    )
    backtest_run.add_argument(
        "--exit-stop-loss-frac",
        type=float,
        default=None,
        help=(
            "Enable stop-loss exit: close when -unrealized PnL >= frac*max_loss "
            "(fallback: frac*abs(entry_cashflow))."
        ),
    )
    backtest_run.add_argument(
        "--exit-max-hold-sessions",
        type=int,
        default=None,
        help="Enable time-stop exit: close when held_sessions >= N.",
    )
    backtest_run.add_argument(
        "--root",
        default=None,
        help="Override repo/data root (default: auto-detect or SPY2_DATA_ROOT).",
    )
    backtest_run.set_defaults(func=_cmd_backtest_run)

    corp_parser = subparsers.add_parser(
        "corpactions",
        help="Corporate actions utilities.",
    )
    corp_sub = corp_parser.add_subparsers(dest="corp_command")
    corp_div = corp_sub.add_parser(
        "dividends",
        help="Ingest a dividend calendar (ex-div dates + amounts).",
    )
    corp_div.add_argument(
        "--symbol",
        default="SPY",
        help="Symbol to ingest (default: SPY).",
    )
    corp_div.add_argument(
        "--start",
        required=False,
        default=None,
        help="Start date YYYY-MM-DD (required for Databento fetch).",
    )
    corp_div.add_argument(
        "--end",
        required=False,
        default=None,
        help="End date YYYY-MM-DD (required for Databento fetch).",
    )
    corp_div.add_argument(
        "--api-key",
        dest="api_key",
        default=None,
        help="Databento API key (falls back to DATABENTO_API_KEY).",
    )
    corp_div.add_argument(
        "--stype-in",
        default="raw_symbol",
        help="Input symbology type (default: raw_symbol).",
    )
    corp_div.add_argument(
        "--pit",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Keep point-in-time history (default: false).",
    )
    corp_div.add_argument(
        "--import-csv",
        dest="import_csv",
        default=None,
        help="Import dividends from a local CSV instead of Databento.",
    )
    corp_div.add_argument(
        "--root",
        default=None,
        help="Override repo/data root (default: auto-detect or SPY2_DATA_ROOT).",
    )
    corp_div.set_defaults(func=_cmd_corpactions_dividends)

    ibkr_parser = subparsers.add_parser("ibkr", help="IBKR utilities.")
    ibkr_sub = ibkr_parser.add_subparsers(dest="ibkr_command")

    ibkr_check = ibkr_sub.add_parser(
        "check",
        help="IBKR connectivity sanity check (no orders).",
    )
    ibkr_check.add_argument(
        "--host",
        default=os.getenv("IBKR_HOST", "127.0.0.1"),
        help=("TWS/IB Gateway host (default: IBKR_HOST env var or 127.0.0.1)."),
    )
    ibkr_check.add_argument(
        "--port",
        type=int,
        help="Override the socket port (defaults depend on --paper/--prod and client type).",
    )
    ibkr_mode = ibkr_check.add_mutually_exclusive_group()
    ibkr_mode.add_argument(
        "--tws",
        action="store_true",
        help="Use TWS ports (paper=7497, production=7496).",
    )
    ibkr_mode.add_argument(
        "--gateway",
        action="store_true",
        help="Use IB Gateway ports (paper=4002, production=4001).",
    )
    ibkr_env = ibkr_check.add_mutually_exclusive_group()
    ibkr_env.add_argument(
        "--paper",
        dest="paper",
        action="store_true",
        default=True,
        help="Use paper account defaults (default).",
    )
    ibkr_env.add_argument(
        "--prod",
        dest="paper",
        action="store_false",
        help="Use production account defaults.",
    )
    ibkr_check.add_argument(
        "--allow-nondefault-port",
        action="store_true",
        help=(
            "Allow non-default ports within supported IBKR port ranges after "
            "manual verification."
        ),
    )
    ibkr_check.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        help="Socket connect timeout in seconds (default: 1.0).",
    )
    ibkr_check.add_argument(
        "--confirm-read-only-unchecked",
        action="store_true",
        help="Confirm Read-Only is disabled in API settings.",
    )
    ibkr_check.set_defaults(func=_cmd_ibkr_check)

    return parser


def _cmd_databento_list_schemas(args: argparse.Namespace) -> int:
    from pathlib import Path

    from spy2.databento import ops as db_ops

    root = Path(args.root).resolve() if args.root else None
    output_path = db_ops.list_schemas_to_artifact(
        dataset=args.dataset,
        api_key=args.api_key,
        root=root,
    )
    print(f"Wrote {output_path}")
    return 0


def _cmd_databento_ingest(args: argparse.Namespace) -> int:
    from pathlib import Path

    from spy2.databento import ops as db_ops

    root = Path(args.root).resolve() if args.root else None
    manifest_path = db_ops.ingest_day(
        date_str=args.date,
        api_key=args.api_key,
        quotes_schema=args.quotes_schema,
        root=root,
        auto_clamp=args.auto_clamp,
    )
    print(f"Wrote {manifest_path}")
    return 0


def _cmd_databento_ingest_range(args: argparse.Namespace) -> int:
    import datetime as dt
    from pathlib import Path

    from spy2.databento import ops as db_ops
    from spy2.common.calendar import trading_sessions

    root = Path(args.root).resolve() if args.root else None
    try:
        start_dt = dt.date.fromisoformat(args.start_date)
        end_dt = dt.date.fromisoformat(args.end_date)
    except ValueError as exc:
        raise SystemExit("Invalid date. Use YYYY-MM-DD.") from exc

    if end_dt < start_dt:
        raise SystemExit("End date must be on or after start date.")

    sessions = trading_sessions(start_dt, end_dt)
    for session in sessions:
        print(f"Ingesting {session.isoformat()} ...")
        manifest_path = db_ops.ingest_day(
            date_str=session.isoformat(),
            api_key=args.api_key,
            quotes_schema=args.quotes_schema,
            root=root,
            auto_clamp=args.auto_clamp,
        )
        print(f"Wrote {manifest_path}")
    return 0


def _cmd_ibkr_check(args: argparse.Namespace) -> int:
    if args.gateway:
        client_name = "IB Gateway"
        paper_default = 4002
        prod_default = 4001
        valid_ports = {4001, 4002}
    else:
        client_name = "TWS"
        paper_default = 7497
        prod_default = 7496
        valid_ports = {7496, 7497}

    default_port = paper_default if args.paper else prod_default
    target_env = "paper" if args.paper else "production"
    port = args.port or default_port
    return ibkr.check_connectivity(
        host=args.host,
        port=port,
        timeout=args.timeout,
        confirm_read_only_unchecked=args.confirm_read_only_unchecked,
        client_name=client_name,
        expected_port=default_port,
        valid_ports=valid_ports,
        allow_nondefault_port=args.allow_nondefault_port,
        target_env=target_env,
    )


def _cmd_data_validate_day(args: argparse.Namespace) -> int:
    from pathlib import Path

    from spy2.data.validation import validate_day

    root = Path(args.root).resolve() if args.root else None
    output_path = validate_day(
        args.date,
        quotes_schema=args.quotes_schema,
        root=root,
    )
    print(f"Wrote {output_path}")
    return 0


def _cmd_snapshots_head(args: argparse.Namespace) -> int:
    import datetime as dt
    import itertools
    from pathlib import Path

    from spy2.options.chain import iter_chain_snapshots

    trade_date = dt.date.fromisoformat(args.date)
    root = Path(args.root).resolve() if args.root else None
    snapshots = iter_chain_snapshots(
        trade_date,
        quotes_schema=args.quotes_schema,
        root=root,
    )
    for idx, snapshot in enumerate(itertools.islice(snapshots, args.n), start=1):
        underlying = (
            "n/a"
            if snapshot.underlying_price is None
            else f"{snapshot.underlying_price:.2f}"
        )
        print(
            f"{idx}. {snapshot.ts_event.isoformat()} "
            f"underlying={underlying} rows={len(snapshot.chain)}"
        )
    return 0


def _cmd_backtest_demo(args: argparse.Namespace) -> int:
    import datetime as dt
    from pathlib import Path

    from spy2.fees.ibkr import IbkrFeeSchedule, estimate_spread_fees
    from spy2.fees.tick import tick_size_for_symbol
    from spy2.options.chain import iter_chain_snapshots
    from spy2.options.fill import fill_vertical_spread
    from spy2.options.selection import priced_spread_from_fill, select_vertical_spread

    trade_date = dt.date.fromisoformat(args.date)
    root = Path(args.root).resolve() if args.root else None
    requested_right = args.right
    target_dt = None
    if args.time_str:
        target_time = dt.time.fromisoformat(args.time_str)
        target_dt = dt.datetime.combine(trade_date, target_time, tzinfo=dt.timezone.utc)

    snapshots = iter_chain_snapshots(
        trade_date,
        quotes_schema=args.quotes_schema,
        root=root,
    )
    chosen = None
    spread = None
    for snapshot in snapshots:
        if target_dt is not None and snapshot.ts_event < target_dt:
            continue

        selection = select_vertical_spread(
            snapshot,
            right=requested_right,
            width=args.width,
            allow_fallback_right=True,
        )
        if selection is None:
            continue
        spread, used_right = selection
        if used_right != requested_right:
            print(f"No {requested_right} rows; falling back to {used_right}.")
            args.right = used_right
        chosen = snapshot
        break

    if chosen is None or spread is None:
        raise SystemExit(
            "No snapshot found with enough strikes to build a vertical spread."
        )

    quotes_by_symbol = {
        row.symbol: (row.bid, row.ask) for row in chosen.chain.itertuples(index=False)
    }
    fill = fill_vertical_spread(
        spread,
        quotes_by_symbol,
        slippage_bps=args.slippage_bps,
        tick_size_fn=tick_size_for_symbol,
    )

    fill_map = {leg.symbol: leg.price for leg in fill.leg_fills}
    priced_spread = priced_spread_from_fill(spread, leg_prices=fill_map)

    net_debit = fill.net_debit
    net_debit_dollars = (
        None if net_debit is None else net_debit * spread.multiplier * spread.quantity
    )
    max_profit = priced_spread.max_profit
    max_loss = priced_spread.max_loss
    max_profit_dollars = (
        None if max_profit is None else max_profit * spread.multiplier * spread.quantity
    )
    max_loss_dollars = (
        None if max_loss is None else max_loss * spread.multiplier * spread.quantity
    )

    print(f"snapshot: {chosen.ts_event.isoformat()}")
    spot = chosen.underlying_price
    if spot is None:
        spot = (spread.long_leg.strike + spread.short_leg.strike) / 2.0
    print(f"underlying: {spot:.2f}")
    print(
        f"legs: LONG {priced_spread.long_leg.symbol} @{priced_spread.long_leg.strike} "
        f"/ SHORT {priced_spread.short_leg.symbol} @{priced_spread.short_leg.strike}"
    )
    fees = estimate_spread_fees(fill, schedule=IbkrFeeSchedule.from_env())
    print(f"net_debit_per_share: {net_debit}")
    print(f"net_debit_dollars: {net_debit_dollars}")
    print(f"max_profit_dollars: {max_profit_dollars}")
    print(f"max_loss_dollars: {max_loss_dollars}")
    print(f"fees_total: {fees.total}")
    return 0


def _cmd_backtest_run(args: argparse.Namespace) -> int:
    import datetime as dt
    from pathlib import Path

    from spy2.backtest.runner import run_backtest_range
    from spy2.portfolio.exits import ExitRuleConfig
    from spy2.options.selection import VerticalSelectionConfig

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    root = Path(args.root).resolve() if args.root else None
    exit_enabled = (
        args.exit_profit_take_frac is not None
        or args.exit_stop_loss_frac is not None
        or args.exit_max_hold_sessions is not None
    )
    exit_rules = ExitRuleConfig(
        enabled=bool(exit_enabled),
        profit_take_frac=args.exit_profit_take_frac,
        stop_loss_frac=args.exit_stop_loss_frac,
        max_hold_sessions=args.exit_max_hold_sessions,
    )
    selection = VerticalSelectionConfig(
        dte_min=args.sel_dte_min,
        dte_max=args.sel_dte_max,
        otm_pct=args.sel_otm_pct,
        min_credit=args.sel_min_credit,
    )
    outputs = run_backtest_range(
        start=start,
        end=end,
        root=root,
        strategy=args.strategy,
        right=args.right,
        width=args.width,
        structure=args.structure,
        quotes_schema=args.quotes_schema,
        slippage_bps=args.slippage_bps,
        initial_cash=args.initial_cash,
        calendar=args.calendar,
        force_close_dte=args.force_close_dte,
        fill_model=args.fill_model,
        fill_alpha=args.fill_alpha,
        fill_sensitivity=args.fill_sensitivity,
        exit_rules=exit_rules,
        selection=selection,
    )
    print(f"Wrote {outputs.trades_path}")
    print(f"Wrote {outputs.equity_curve_path}")
    print(f"Wrote {outputs.summary_path}")
    return 0


def _cmd_corpactions_dividends(args: argparse.Namespace) -> int:
    from pathlib import Path

    from spy2.corpactions import ops as ca_ops

    root = Path(args.root).resolve() if args.root else None

    if args.import_csv:
        output_path, manifest_path = ca_ops.import_dividends_csv(
            symbol=args.symbol,
            csv_path=Path(args.import_csv).expanduser().resolve(),
            root=root,
        )
    else:
        if not args.start or not args.end:
            raise SystemExit("--start and --end are required for Databento fetch.")
        output_path, manifest_path = ca_ops.ingest_dividends(
            symbol=args.symbol,
            start_date=args.start,
            end_date=args.end,
            api_key=args.api_key,
            stype_in=args.stype_in,
            pit=args.pit,
            root=root,
        )

    print(f"Wrote {output_path}")
    print(f"Wrote {manifest_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    _maybe_load_dotenv()

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        try:
            print(metadata.version("spy-2"))
        except metadata.PackageNotFoundError:
            print("spy-2 (version unknown)")
        return 0

    if not getattr(args, "command", None):
        parser.print_help()
        return 0

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


def _maybe_load_dotenv() -> None:
    import os

    # Keep tests hermetic by default.
    if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("SPY2_DISABLE_DOTENV"):
        return

    try:
        from dotenv import load_dotenv  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return

    from spy2.common.paths import repo_root

    dotenv_path = repo_root() / ".env"
    if dotenv_path.is_file():
        load_dotenv(dotenv_path=str(dotenv_path), override=False)


if __name__ == "__main__":
    raise SystemExit(main())
