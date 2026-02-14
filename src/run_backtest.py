# src/run_backtest.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.data.loaders import load_spot
from src.backtest.backtest import backtest_hold_to_expiry


def main():
    opp_path = "reports/tables/opportunities.csv"
    if not Path(opp_path).exists():
        raise FileNotFoundError(f"Run scanner first: {opp_path} not found")

    opp = pd.read_csv(opp_path)
    spot = load_spot("data/raw/spot.csv")

    bt = backtest_hold_to_expiry(opp, spot_df=spot)

    Path("reports/tables").mkdir(parents=True, exist_ok=True)
    out_path = "reports/tables/backtest.csv"
    bt.to_csv(out_path, index=False)

    print(f"Saved {len(bt)} trades -> {out_path}")
    if len(bt) > 0:
        print("Final cum_pnl:", bt["cum_pnl"].iloc[-1])


if __name__ == "__main__":
    main()
