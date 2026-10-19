import sys
import time
from multiprocessing import Pool

import pandas as pd
from clean import process_station
from serial_pipeline import OUT, load


def run_serial(groups):
    return [process_station(g) for g in groups]


def run_parallel(groups, workers):
    chunk = max(1, len(groups) // (workers * 4))      # stations handed to a worker at a time
    with Pool(workers) as pool:
        return pool.map(process_station, groups, chunksize=chunk)


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    df = load()
    groups = [g for _, g in df.groupby("StationId")]  # partition by station
    print(f"Rows: {len(df):,} | Stations: {len(groups)} | Workers: {workers}")

    t0 = time.perf_counter()
    serial = pd.concat(run_serial(groups), ignore_index=True)
    t_serial = time.perf_counter() - t0

    t0 = time.perf_counter()
    parallel = pd.concat(run_parallel(groups, workers), ignore_index=True)
    t_parallel = time.perf_counter() - t0

    pd.testing.assert_series_equal(serial["AQI_calc"], parallel["AQI_calc"])
    print("Correctness check passed: parallel output == serial output")

    OUT.mkdir(parents=True, exist_ok=True)
    parallel.to_parquet(OUT / "station_day_clean.parquet", index=False)

    print(f"\nSerial:   {t_serial:.3f} s")
    print(f"Parallel: {t_parallel:.3f} s ({workers} workers)")
    print(f"Speedup:  {t_serial / t_parallel:.2f}x")
    print("(Below 1x is expected here: the workload is too small to beat process start-up cost.)")


if __name__ == "__main__":
    main()