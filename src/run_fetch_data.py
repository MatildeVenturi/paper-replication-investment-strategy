from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

#call API and save data.raw
from src.data.fetch_deribit import (
    fetch_spot_index,
    fetch_vanilla_snapshot,
    pick_next_expiry,
)
from src.data.fetch_polymarket import fetch_crypto_threshold_markets_for_expiry



def main() -> None:
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    underlying = "BTC"  # or "ETH"

    # decision date (paper: 08:00 UTC; we store day key)
    date_iso = datetime.now(timezone.utc).date().isoformat()

    # choose a Deribit expiry that ACTUALLY exists
    min_expiry_iso = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
    expiry_iso = pick_next_expiry(underlying, min_expiry_iso)
    print(f"Using expiry: {expiry_iso}")

   
    spot = float(fetch_spot_index(underlying))
    pd.DataFrame(
        [{"date": date_iso, "underlying": underlying, "spot": spot}]
    ).to_csv("data/raw/spot.csv", index=False)
    print("Wrote data/raw/spot.csv")

    vanilla_rows = fetch_vanilla_snapshot(
        currency=underlying,
        expiry_iso=expiry_iso,
        date_iso=date_iso,
        max_strikes=80,
    )
    pd.DataFrame(
        vanilla_rows,
        columns=["date", "underlying", "expiry", "strike", "type", "price"],
    ).to_csv("data/raw/vanilla.csv", index=False)
    print(f"Wrote data/raw/vanilla.csv ({len(vanilla_rows)} rows)")


    binary_rows = fetch_crypto_threshold_markets_for_expiry(
    currency=underlying,
    expiry_iso=expiry_iso,
    limit=400,
)


    if binary_rows:
        bdf = pd.DataFrame(binary_rows)
        bdf["date"] = date_iso
        bdf = bdf[["date", "underlying", "expiry", "strike", "price"]]
        bdf.to_csv("data/raw/binary.csv", index=False)
        print(f"Wrote data/raw/binary.csv ({len(bdf)} rows)")
    else:
        pd.DataFrame(
            columns=["date", "underlying", "expiry", "strike", "price"]
        ).to_csv("data/raw/binary.csv", index=False)
        print("Wrote data/raw/binary.csv (0 rows)")

    print("\nNext:")
    print("  python3 -m src.run_scan")


if __name__ == "__main__":
    main()
