from __future__ import annotations

from pathlib import Path

from src.data.loaders import load_spot, load_binary, load_vanilla
from src.arbitrage.scanner import scan_opportunities


def main() -> None:
    # Load inputs
    spot = load_spot("data/raw/spot.csv")
    binary = load_binary("data/raw/binary.csv")
    vanilla = load_vanilla("data/raw/vanilla.csv")

    # Quick sanity prints
    print(f"[INFO] spot rows   : {len(spot)}")
    print(f"[INFO] binary rows : {len(binary)}")
    print(f"[INFO] vanilla rows: {len(vanilla)}")

    # Run scan with relaxed settings
    opp = scan_opportunities(
        spot_df=spot,
        binary_df=binary,
        vanilla_df=vanilla,
        Qv=1.0,
        fee_usd=0.0,
        min_edge=0.0,
        # --- NEW knobs (less strict) ---
        edge_epsilon=100.0,        # try 50 / 100 / 200
        pb_clip=0.02,              # try 0.01 if you have very extreme probabilities
        nearest_expiry_days=7,     # try 2 for strict, 7 for more matches
    )

    # Save output
    Path("reports/tables").mkdir(parents=True, exist_ok=True)
    out_path = "reports/tables/opportunities.csv"
    opp.to_csv(out_path, index=False)

    print(f"[INFO] Saved {len(opp)} opportunities -> {out_path}")

    # Optional: show top few rows in terminal
    if len(opp) > 0:
        print("\n[INFO] Top opportunities:")
        print(opp.head(10).to_string(index=False))


if __name__ == "__main__":
    main()