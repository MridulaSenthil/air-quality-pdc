import json
import time
from pathlib import Path

import pandas as pd
from clean import process_station

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed"


def load():
    df = pd.read_csv(RAW / "station_day.csv", parse_dates=["Date"])
    stations = pd.read_csv(RAW / "stations.csv")
    return df.merge(
        stations[["StationId", "StationName", "City", "State"]],
        on="StationId", how="left",
    )


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    df = load()
    groups = [g for _, g in df.groupby("StationId")]   # partition by station
    print(f"Rows: {len(df):,} | Stations: {len(groups)}")

    t0 = time.perf_counter()
    results = [process_station(g) for g in groups]     # SERIAL: one at a time
    elapsed = time.perf_counter() - t0

    clean = pd.concat(results, ignore_index=True)
    clean.to_parquet(OUT / "station_day_clean.parquet", index=False)

    print(f"\nSerial processing time: {elapsed:.3f} s")
    print(f"Missing AQI  before: {df['AQI'].isna().mean():.1%} | after: {clean['AQI_calc'].isna().mean():.1%}")
    both = clean[["AQI", "AQI_calc"]].dropna()
    print(f"Correlation with dataset AQI: {both['AQI'].corr(both['AQI_calc']):.3f}")

    top = (clean.groupby(["StationId", "StationName", "City"], dropna=False)["AQI_calc"]
                .mean().sort_values(ascending=False).head(10).round(1))
    print("\nTop 10 most polluted stations (mean AQI):")
    print(top.to_string())

    (ROOT / "benchmarks").mkdir(exist_ok=True)
    (ROOT / "benchmarks" / "serial_baseline.json").write_text(
        json.dumps({"stations": len(groups), "rows": len(df), "serial_seconds": elapsed}, indent=2)
    )


if __name__ == "__main__":
    main()