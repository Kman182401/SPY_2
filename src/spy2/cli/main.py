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
