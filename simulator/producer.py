import argparse
import json
import math
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parents[1]
POLLUTANTS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "O3", "NH3"]
EVENT_POLLUTANTS = {"PM2.5", "PM10", "NO2"}


def load_profiles():
    df = pd.read_parquet(ROOT / "data" / "processed" / "station_day_clean.parquet")
    meta = df.groupby("StationId")[["StationName", "City"]].first()
    means = df.groupby("StationId")[POLLUTANTS].mean()
    means = means.fillna(means.median()).fillna(1.0)
    profiles = meta.join(means)
    profiles["City"] = profiles["City"].fillna("Unknown")
    return profiles


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=2.0, help="seconds between ticks (0 = max speed)")
    ap.add_argument("--ticks", type=int, default=0, help="stop after N ticks (0 = run forever)")
    args = ap.parse_args()

    profiles = load_profiles()
    cities = sorted(profiles["City"].unique())
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    sim_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    events = {}                                    # city -> [multiplier, ticks_left]
    tick = 0
    print(f"Simulating {len(profiles)} stations in {len(cities)} cities. Ctrl+C to stop.")

    try:
        while args.ticks == 0 or tick < args.ticks:
            tick += 1
            sim_time = datetime.now().replace(microsecond=0)
            if random.random() < 0.15:             # start a new pollution event
                city = random.choice(cities)
                events[city] = [random.uniform(2.5, 4.0), 3]
                print(f"  >>> POLLUTION EVENT in {city} (x{events[city][0]:.1f})")

            diurnal = 1 + 0.25 * math.cos(2 * math.pi * (sim_time.hour - 9) / 12)  # peaks ~9am and ~9pm
            for sid, row in profiles.iterrows():
                mult = events.get(row["City"], [1.0, 0])[0]
                readings = {}
                for p in POLLUTANTS:
                    v = row[p] * diurnal * math.exp(random.gauss(0, 0.15))
                    if p in EVENT_POLLUTANTS:
                        v *= mult
                    readings[p] = round(v, 2)
                msg = {"station_id": sid, "station_name": row["StationName"],
                       "city": row["City"], "ts": sim_time.isoformat(), "readings": readings}
                producer.produce("aq.readings", key=sid, value=json.dumps(msg))
                producer.poll(0)
            producer.flush()

            for city in list(events):
                events[city][1] -= 1
                if events[city][1] <= 0:
                    del events[city]

            print(f"tick {tick:4d} | sim {sim_time:%Y-%m-%d %H:%M} | sent {len(profiles)} readings")
            sim_time += timedelta(hours=1)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print("Producer stopped.")


if __name__ == "__main__":
    main()