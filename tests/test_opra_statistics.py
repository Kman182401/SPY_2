import datetime as dt

import pandas as pd

from spy2.options.chain import load_option_statistics


def _write_parquet(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_load_option_statistics_extracts_open_interest_and_volume(tmp_path):
    trade_date = dt.date(2026, 2, 2)
    date_str = trade_date.isoformat()

    symbols = ["OPT1", "OPT2"]
    ts = pd.Timestamp("2026-02-02T20:59:00Z")
    stats = pd.DataFrame(
        {
            "ts_event": [ts, ts, ts, ts],
            "symbol": [symbols[0], symbols[0], symbols[1], symbols[1]],
            "stat_type": [6, 9, 6, 9],
            "quantity": [123, 456, 789, 1011],
        }
    )
    _write_parquet(
        stats,
        tmp_path
        / "data"
        / "raw"
        / "OPRA.PILLAR"
        / "statistics"
        / f"date={date_str}"
        / "part-0000.parquet",
    )

    df = load_option_statistics(trade_date, root=tmp_path)
    by_symbol = df.set_index("symbol").to_dict(orient="index")
    assert by_symbol["OPT1"]["open_interest"] == 456
    assert by_symbol["OPT1"]["volume"] == 123
    assert by_symbol["OPT2"]["open_interest"] == 1011
    assert by_symbol["OPT2"]["volume"] == 789
