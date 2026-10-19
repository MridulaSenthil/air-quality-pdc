import argparse
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "batch"))
from aqi import LEVEL_NAMES, aqi_level, aqi_single


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=f"worker-{os.getpid()}")
    args = ap.parse_args()

    def on_assign(consumer, partitions):
        print(f"[{args.id}] partitions assigned: {sorted(p.partition for p in partitions)}")

    def on_revoke(consumer, partitions):
        print(f"[{args.id}] partitions revoked: {sorted(p.partition for p in partitions)}")

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "aq-workers",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(["aq.readings"], on_assign=on_assign, on_revoke=on_revoke)
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    state = {}                                    # station_id -> (last_aqi, last_level)
    count, t0 = 0, time.perf_counter()
    print(f"[{args.id}] started, waiting for readings...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{args.id}] error: {msg.error()}")
                continue

            data = json.loads(msg.value())
            aqi, main_pollutant = aqi_single(data["readings"])
            if aqi is None:
                continue
            level = aqi_level(aqi)
            sid = data["station_id"]
            prev = state.get(sid)
            state[sid] = (aqi, level)

            processed = {
                "station_id": sid, "station_name": data["station_name"], "city": data["city"],
                "ts": data["ts"], "aqi": aqi, "bucket": LEVEL_NAMES[level],
                "main_pollutant": main_pollutant, "readings": data["readings"],
                "worker": args.id, "partition": msg.partition(),
            }
            producer.produce("aq.processed", key=sid, value=json.dumps(processed))

            alert_type = None
            if prev:
                if level >= 3 and level > prev[1]:
                    alert_type = "ESCALATION"
                elif aqi - prev[0] >= 100:
                    alert_type = "SPIKE"
            if alert_type:
                alert = {
                    "type": alert_type, "station_id": sid, "station_name": data["station_name"],
                    "city": data["city"], "ts": data["ts"], "aqi": aqi,
                    "level": LEVEL_NAMES[level], "main_pollutant": main_pollutant,
                    "worker": args.id,
                }
                producer.produce("aq.alerts", key=sid, value=json.dumps(alert))
                print(f"[{args.id}] ALERT {alert_type}: {data['city']} / {data['station_name']} "
                      f"AQI {aqi} ({LEVEL_NAMES[level]}, {main_pollutant})")
            producer.poll(0)

            count += 1
            if count % 200 == 0:
                rate = count / (time.perf_counter() - t0)
                print(f"[{args.id}] processed {count} readings ({rate:.0f}/s)")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()                           # leave the group cleanly so Kafka rebalances fast
        producer.flush()
        print(f"[{args.id}] stopped after {count} readings")


if __name__ == "__main__":
    main()