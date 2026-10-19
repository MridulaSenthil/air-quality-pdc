import json
import statistics
import sys
import time
from multiprocessing import Pool
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "batch"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from clean import process_station
from serial_pipeline import load

WORKERS = [2, 4, 8]
REPEATS = 3


def run_serial(groups):
    return [process_station(g) for g in groups]


def run_parallel(groups, workers):
    chunk = max(1, len(groups) // (workers * 4))
    with Pool(workers) as pool:
        return pool.map(process_station, groups, chunksize=chunk)


def timed(fn, *args):
    t0 = time.perf_counter()
    fn(*args)
    return time.perf_counter() - t0


def main():
    scale = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    df = load()
    base = [g for _, g in df.groupby("StationId")]
    groups = base * scale                              # repeat the station set to create a bigger workload
    rows = sum(len(g) for g in groups)
    print(f"Workload: {len(groups):,} station-chunks, {rows:,} rows (scale x{scale})\n")

    run_serial(base[:5])                               # warm-up

    serial_t = statistics.median(timed(run_serial, groups) for _ in range(REPEATS))
    print(f"Serial          : {serial_t:7.2f} s")

    results = {"scale": scale, "rows": rows, "serial_seconds": serial_t, "runs": []}
    for w in WORKERS:
        t = statistics.median(timed(run_parallel, groups, w) for _ in range(REPEATS))
        speedup = serial_t / t
        eff = speedup / w
        results["runs"].append({"workers": w, "seconds": t, "speedup": speedup, "efficiency": eff})
        print(f"{w} workers       : {t:7.2f} s | speedup {speedup:4.2f}x | efficiency {eff:5.1%}")

    last = results["runs"][-1]
    p, s = last["workers"], last["speedup"]
    results["amdahl_serial_fraction"] = (1 / s - 1 / p) / (1 - 1 / p)
    print(f"\nEstimated serial fraction (Amdahl): {results['amdahl_serial_fraction']:.1%}")

    (ROOT / "benchmarks" / "results.json").write_text(json.dumps(results, indent=2))

    xs = [1] + WORKERS
    ys = [1.0] + [r["speedup"] for r in results["runs"]]
    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    ax[0].plot(xs, ys, "o-", label="Measured")
    ax[0].plot(xs, xs, "--", label="Ideal (linear)")
    ax[0].set(xlabel="Workers", ylabel="Speedup", title="Speedup vs workers")
    ax[0].legend()
    ax[1].bar([str(w) for w in WORKERS], [r["efficiency"] * 100 for r in results["runs"]])
    ax[1].set(xlabel="Workers", ylabel="Efficiency (%)", title="Parallel efficiency")
    plt.tight_layout()
    plt.savefig(ROOT / "benchmarks" / "speedup.png", dpi=150)
    print("Saved benchmarks/results.json and benchmarks/speedup.png")


if __name__ == "__main__":
    main()