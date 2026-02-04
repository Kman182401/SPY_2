from __future__ import annotations

import argparse
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
    db_ingest.set_defaults(func=_cmd_databento_ingest)

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

    ibkr_parser = subparsers.add_parser("ibkr", help="IBKR utilities.")
    ibkr_sub = ibkr_parser.add_subparsers(dest="ibkr_command")

    ibkr_check = ibkr_sub.add_parser(
        "check",
        help="Paper connectivity sanity check (no orders).",
    )
    ibkr_check.add_argument(
        "--host",
        default="127.0.0.1",
        help="TWS/IB Gateway host (default: 127.0.0.1).",
    )
    ibkr_check.add_argument(
        "--port",
        type=int,
        help="Override the socket port (defaults to paper ports).",
    )
    ibkr_mode = ibkr_check.add_mutually_exclusive_group()
    ibkr_mode.add_argument(
        "--tws",
        action="store_true",
        help="Use TWS paper default port (7497).",
    )
    ibkr_mode.add_argument(
        "--gateway",
        action="store_true",
        help="Use IB Gateway paper default port (4002).",
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
    from spy2.databento import ops as db_ops

    output_path = db_ops.list_schemas_to_artifact(
        dataset=args.dataset,
        api_key=args.api_key,
    )
    print(f"Wrote {output_path}")
    return 0


def _cmd_databento_ingest(args: argparse.Namespace) -> int:
    from spy2.databento import ops as db_ops

    manifest_path = db_ops.ingest_day(
        date_str=args.date,
        api_key=args.api_key,
        quotes_schema=args.quotes_schema,
    )
    print(f"Wrote {manifest_path}")
    return 0


def _cmd_ibkr_check(args: argparse.Namespace) -> int:
    if args.gateway:
        default_port = 4002
        client_name = "IB Gateway"
    else:
        default_port = 7497
        client_name = "TWS"

    port = args.port or default_port
    return ibkr.check_connectivity(
        host=args.host,
        port=port,
        timeout=args.timeout,
        confirm_read_only_unchecked=args.confirm_read_only_unchecked,
        client_name=client_name,
        expected_port=default_port,
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
    import dataclasses
    import datetime as dt
    from pathlib import Path

    from spy2.options.chain import iter_chain_snapshots
    from spy2.options.fill import fill_vertical_spread
    from spy2.options.models import OptionLeg, VerticalSpread

    trade_date = dt.date.fromisoformat(args.date)
    root = Path(args.root).resolve() if args.root else None
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
    for snapshot in snapshots:
        if target_dt is None or snapshot.ts_event >= target_dt:
            chosen = snapshot
            break
    if chosen is None:
        raise SystemExit("No snapshots found for the requested date/time.")

    chain = chosen.chain
    chain = chain.dropna(subset=["symbol", "expiration", "strike", "right"])
    chain = chain[chain["right"] == args.right]
    if chain.empty:
        raise SystemExit("No option chain rows matched the requested right.")

    expirations = sorted(set(chain["expiration"]))
    expiration = expirations[0]
    subset = chain[chain["expiration"] == expiration]
    strikes = sorted(set(subset["strike"]))
    if not strikes:
        raise SystemExit("No strikes found for the selected expiration.")

    spot = chosen.underlying_price
    if spot is None:
        spot = strikes[len(strikes) // 2]

    if args.right == "C":
        long_candidates = [strike for strike in strikes if strike >= spot]
        long_strike = long_candidates[0] if long_candidates else strikes[-1]
        short_strike = long_strike + args.width
        if short_strike not in strikes:
            higher = [strike for strike in strikes if strike > long_strike]
            if not higher:
                raise SystemExit("No higher strike available for call spread.")
            short_strike = min(higher, key=lambda strike: abs(strike - short_strike))
    else:
        long_candidates = [strike for strike in strikes if strike <= spot]
        long_strike = long_candidates[-1] if long_candidates else strikes[0]
        short_strike = long_strike - args.width
        if short_strike not in strikes:
            lower = [strike for strike in strikes if strike < long_strike]
            if not lower:
                raise SystemExit("No lower strike available for put spread.")
            short_strike = max(lower, key=lambda strike: abs(strike - short_strike))

    long_row = subset[subset["strike"] == long_strike].iloc[0]
    short_row = subset[subset["strike"] == short_strike].iloc[0]

    long_leg = OptionLeg(
        symbol=long_row.symbol,
        right=args.right,
        expiration=long_row.expiration,
        strike=float(long_row.strike),
        side=1,
        quantity=1,
    )
    short_leg = OptionLeg(
        symbol=short_row.symbol,
        right=args.right,
        expiration=short_row.expiration,
        strike=float(short_row.strike),
        side=-1,
        quantity=1,
    )
    spread = VerticalSpread.from_legs(long_leg, short_leg)

    quotes_by_symbol = {
        row.symbol: (row.bid, row.ask) for row in subset.itertuples(index=False)
    }
    fill = fill_vertical_spread(
        spread,
        quotes_by_symbol,
        slippage_bps=args.slippage_bps,
    )

    fill_map = {leg.symbol: leg for leg in fill.leg_fills}
    priced_long = dataclasses.replace(
        spread.long_leg, price=fill_map[spread.long_leg.symbol].price
    )
    priced_short = dataclasses.replace(
        spread.short_leg, price=fill_map[spread.short_leg.symbol].price
    )
    priced_spread = VerticalSpread.from_legs(
        priced_long,
        priced_short,
        multiplier=spread.multiplier,
    )

    net_debit = fill.net_debit
    net_debit_dollars = None if net_debit is None else net_debit * spread.multiplier
    max_profit = priced_spread.max_profit
    max_loss = priced_spread.max_loss
    max_profit_dollars = None if max_profit is None else max_profit * spread.multiplier
    max_loss_dollars = None if max_loss is None else max_loss * spread.multiplier

    print(f"snapshot: {chosen.ts_event.isoformat()}")
    print(f"underlying: {spot:.2f}")
    print(
        f"legs: LONG {priced_long.symbol} @{priced_long.strike} "
        f"/ SHORT {priced_short.symbol} @{priced_short.strike}"
    )
    print(f"net_debit_per_share: {net_debit}")
    print(f"net_debit_dollars: {net_debit_dollars}")
    print(f"max_profit_dollars: {max_profit_dollars}")
    print(f"max_loss_dollars: {max_loss_dollars}")
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

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


if __name__ == "__main__":
    raise SystemExit(main())
