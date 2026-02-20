from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from src.data.fetch_deribit import (
    fetch_spot_index,
    fetch_vanilla_snapshot,
    pick_expiries_in_window,
)

from src.data.fetch_polymarket import fetch_crypto_threshold_markets


def main() -> None:
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    underlying = "BTC"  # or "ETH"
    today = datetime.now(timezone.utc).date()
    date_iso = today.isoformat()

    # -------------------------------------------------
    # 1) DERIBIT EXPIRIES: from +7 days to +1 year
    # -------------------------------------------------
    min_expiry_iso = (today + timedelta(days=7)).isoformat()

    expiries = pick_expiries_in_window(
        underlying,
        start_expiry_iso=min_expiry_iso,
        window_days=365,
    )

    if not expiries:
        raise RuntimeError(
            f"No Deribit expiries found in the next year from {min_expiry_iso} for {underlying}"
        )

    print(
        f"Using Deribit expiries (count={len(expiries)}): "
        f"{expiries[:10]}{' ...' if len(expiries) > 10 else ''}",
        flush=True,
    )

    # -------------------------------------------------
    # 2) SPOT
    # -------------------------------------------------
    spot = float(fetch_spot_index(underlying))
    pd.DataFrame(
        [{"date": date_iso, "underlying": underlying, "spot": spot}]
    ).to_csv("data/raw/spot.csv", index=False)
    print("Wrote data/raw/spot.csv", flush=True)

    # -------------------------------------------------
    # 3) VANILLA OPTIONS (Deribit)
    # -------------------------------------------------
    all_vanilla_rows: list[dict] = []

    for expiry_iso in expiries:
        rows = fetch_vanilla_snapshot(
            currency=underlying,
            expiry_iso=expiry_iso,
            date_iso=date_iso,
            max_strikes=60,   # 60 is safer than 80 for rate limits
        )
        all_vanilla_rows.extend(rows)

    vdf = pd.DataFrame(
        all_vanilla_rows,
        columns=["date", "underlying", "expiry", "strike", "type", "price"],
    )
    vdf.to_csv("data/raw/vanilla.csv", index=False)
    print(f"Wrote data/raw/vanilla.csv ({len(vdf)} rows)", flush=True)

    # -------------------------------------------------
    # 4) POLYMARKET (future-only, paginated)
    # -------------------------------------------------
    pm_all = fetch_crypto_threshold_markets(
        currency=underlying,
        active=True,
        closed=False,
        page_limit=200,     # page size
        max_pages=60,       # scan up to 60 * 200 markets
        require_orderbook=False,
        future_only=True,
    )

    print("Polymarket total rows (future only):", len(pm_all), flush=True)

    pm_expiries = sorted({r["expiry"] for r in pm_all})
    print("Polymarket unique expiries (sample):", pm_expiries[:30], flush=True)

    # -------------------------------------------------
    # 5) INTERSECT EXPIRIES
    # -------------------------------------------------
    common = sorted(set(expiries).intersection(set(pm_expiries)))
    print("Common expiries Deribit ∩ Polymarket:", common[:30], flush=True)

    if common:
        all_binary_rows = [r for r in pm_all if r["expiry"] in common]
    else:
        print(
            "[WARN] No common expiries. Writing ALL future Polymarket rows (unfiltered).",
            flush=True,
        )
        all_binary_rows = pm_all

    # -------------------------------------------------
    # 6) WRITE binary.csv
    # -------------------------------------------------
    if all_binary_rows:
        bdf = pd.DataFrame(all_binary_rows)
        bdf["date"] = date_iso
        bdf = bdf[["date", "underlying", "expiry", "strike", "price"]]
    else:
        bdf = pd.DataFrame(
            columns=["date", "underlying", "expiry", "strike", "price"]
        )

    bdf.to_csv("data/raw/binary.csv", index=False)
    print(f"Wrote data/raw/binary.csv ({len(bdf)} rows)", flush=True)

    print("\nNext:", flush=True)
    print("  python3 -m src.run_scan", flush=True)


if __name__ == "__main__":
    main()