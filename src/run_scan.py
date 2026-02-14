# src/run_scan.py
from __future__ import annotations

from pathlib import Path

from src.data.loaders import load_spot, load_binary, load_vanilla
from src.arbitrage.scanner import scan_opportunities


def main():
    spot = load_spot("data/raw/spot.csv")
    binary = load_binary("data/raw/binary.csv")
    vanilla = load_vanilla("data/raw/vanilla.csv")

    opp = scan_opportunities(
        spot_df=spot,
        binary_df=binary,
        vanilla_df=vanilla,
        Qv=1.0,
        fee_usd=0.0,
        min_edge=0.0,
    )

    Path("reports/tables").mkdir(parents=True, exist_ok=True)
    out_path = "reports/tables/opportunities.csv"
    opp.to_csv(out_path, index=False)

    print(f"Saved {len(opp)} opportunities -> {out_path}")


if __name__ == "__main__":
    main()
