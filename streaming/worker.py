import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "batch"))
from aqi import LEVEL_NAMES, aqi_level, aqi_single

SPIKE_RATIO = 1.5    # alert if AQI >= 1.5x the station's running average
COOLDOWN = 12        # readings to wait before re-alerting the same station
WARMUP = 12          # readings needed to build a baseline first
EMA_ALPHA = 0.1


def _stop(*_):
    raise KeyboardInterrupt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=f"worker-{os.getpid()}")
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, _stop)

    def on_assign(consumer, partitions):
        print(f"[{args.id}] partitions assigned: {sorted(p.partition for p in partitions)}", flush=True)

    def on_revoke(consumer, partitions):
        print(f"[{args.id}] partitions revoked: {sorted(p.partition for p in partitions)}", flush=True)

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "aq-workers",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "session.timeout.ms": 10000,      # a crashed worker is detected in ~10 s
    })
    consumer.subscribe(["aq.readings"], on_assign=on_assign, on_revoke=on_revoke)
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    state = {}                            # station_id -> per-station alert state
    count, t0 = 0, time.perf_counter()
    print(f"[{args.id}] started, waiting for readings...", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{args.id}] error: {msg.error()}", flush=True)
                continue

            data = json.loads(msg.value())
            aqi, main_pollutant = aqi_single(data["readings"])
            if aqi is None:
                continue
            level = aqi_level(aqi)
            sid = data["station_id"]

            st = state.setdefault(sid, {"ema": None, "n": 0, "level": None,
                                        "last_n": -10**9, "last_level": -1})
            st["n"] += 1

            alert_type = None
            if st["ema"] is not None and st["n"] > WARMUP:
                if level >= 3 and aqi >= SPIKE_RATIO * st["ema"]:
                    alert_type = "ANOMALY"
                elif level == 5 and st["level"] is not None and st["level"] < 5:
                    alert_type = "SEVERE"
            if alert_type and st["n"] - st["last_n"] < COOLDOWN and level <= st["last_level"]:
                alert_type = None         # cooldown: suppress repeats

            st["ema"] = aqi if st["ema"] is None else (1 - EMA_ALPHA) * st["ema"] + EMA_ALPHA * aqi
            st["level"] = level

            processed = {
                "station_id": sid, "station_name": data["station_name"], "city": data["city"],
                "ts": data["ts"], "aqi": aqi, "bucket": LEVEL_NAMES[level],
                "main_pollutant": main_pollutant, "readings": data["readings"],
                "worker": args.id, "partition": msg.partition(),
            }
            producer.produce("aq.processed", key=sid, value=json.dumps(processed))

            if alert_type:
                st["last_n"], st["last_level"] = st["n"], level
                alert = {
                    "type": alert_type, "station_id": sid, "station_name": data["station_name"],
                    "city": data["city"], "ts": data["ts"], "aqi": aqi,
                    "level": LEVEL_NAMES[level], "main_pollutant": main_pollutant, "worker": args.id,
                }
                producer.produce("aq.alerts", key=sid, value=json.dumps(alert))
                print(f"[{args.id}] ALERT {alert_type}: {data['city']} / {data['station_name']} "
                      f"AQI {aqi} ({LEVEL_NAMES[level]}, {main_pollutant})", flush=True)
            producer.poll(0)

            count += 1
            if count % 500 == 0:
                print(f"[{args.id}] processed {count} readings ({count / (time.perf_counter() - t0):.0f}/s)", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()                  # leave the group cleanly so Kafka rebalances at once
        producer.flush()
        print(f"[{args.id}] stopped after {count} readings", flush=True)


if __name__ == "__main__":
    main()