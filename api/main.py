import asyncio
import json
import queue
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

from cities import station_coords

ROOT = Path(__file__).resolve().parents[1]
POLLS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "O3"]

lock = threading.Lock()
live = {}                                   # station_id -> latest processed reading
alerts = deque(maxlen=300)
pending_r, pending_a = [], []
clients = set()
workers = {}                                # worker id -> {count, last, parts}
totals = {"alerts": 0, "readings": 0, "rate": 0, "sim_time": None}
rate_hist = deque(maxlen=5)
db_q = queue.Queue()
HIST, TRENDS = None, []


# ---------- Kafka -> in-memory state (+ DB queue) ----------
def consume():
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "aq-api",
                  "auto.offset.reset": "latest", "enable.auto.commit": True})
    c.subscribe(["aq.processed", "aq.alerts"])
    while True:
        msg = c.poll(0.5)
        if msg is None or msg.error():
            continue
        d = json.loads(msg.value())
        now = time.time()
        with lock:
            if msg.topic() == "aq.processed":
                d["lat"], d["lon"] = station_coords(d["station_id"], d["city"])
                live[d["station_id"]] = d
                pending_r.append(d)
                totals["readings"] += 1
                totals["sim_time"] = d["ts"]
                w = workers.setdefault(d["worker"], {"count": 0, "last": now, "parts": {}})
                w["count"] += 1
                w["last"] = now
                w["parts"][d["partition"]] = now
                db_q.put(("r", (d["ts"], d["station_id"], d["station_name"], d["city"], d["aqi"],
                                d["bucket"], d["main_pollutant"], d["worker"])))
            else:
                alerts.appendleft(d)
                pending_a.append(d)
                totals["alerts"] += 1
                db_q.put(("a", (d["ts"], d["type"], d["station_id"], d["station_name"], d["city"],
                                d["aqi"], d["level"], d["main_pollutant"], d["worker"])))


# ---------- Postgres writer (optional: API still works if DB is down) ----------
def db_thread():
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        conn = psycopg2.connect(host="localhost", port=5432, user="aq", password="aqpass", dbname="airquality")
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS readings(
            id BIGSERIAL PRIMARY KEY, sim_ts TIMESTAMP, station_id TEXT, station_name TEXT, city TEXT,
            aqi INT, bucket TEXT, main_pollutant TEXT, worker TEXT, received_at TIMESTAMPTZ DEFAULT now())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS alerts(
            id BIGSERIAL PRIMARY KEY, sim_ts TIMESTAMP, type TEXT, station_id TEXT, station_name TEXT,
            city TEXT, aqi INT, level TEXT, main_pollutant TEXT, worker TEXT,
            received_at TIMESTAMPTZ DEFAULT now())""")
    except Exception as e:
        print("[db] disabled:", e)
        return
    while True:
        time.sleep(2)
        rr, aa = [], []
        while True:
            try:
                kind, row = db_q.get_nowait()
            except queue.Empty:
                break
            (rr if kind == "r" else aa).append(row)
        try:
            if rr:
                execute_values(cur, "INSERT INTO readings (sim_ts,station_id,station_name,city,aqi,bucket,"
                                    "main_pollutant,worker) VALUES %s", rr)
            if aa:
                execute_values(cur, "INSERT INTO alerts (sim_ts,type,station_id,station_name,city,aqi,level,"
                                    "main_pollutant,worker) VALUES %s", aa)
        except Exception as e:
            print("[db] write failed:", e)


# ---------- stats + websocket broadcast ----------
def stats():
    now = time.time()
    with lock:
        vals = list(live.values())
        ws = [{"id": k, "online": now - v["last"] < 10, "count": v["count"],
               "partitions": sorted(p for p, t in v["parts"].items() if now - t < 10)}
              for k, v in sorted(workers.items())]
        out = {"stations": len(vals), "alerts_total": totals["alerts"], "rate": totals["rate"],
               "sim_time": totals["sim_time"], "workers": ws}
    out["avg_aqi"] = round(sum(v["aqi"] for v in vals) / len(vals)) if vals else 0
    if vals:
        w = max(vals, key=lambda v: v["aqi"])
        out["worst"] = {"station": w["station_name"], "city": w["city"], "aqi": w["aqi"]}
    else:
        out["worst"] = None
    return out


async def broadcaster():
    while True:
        await asyncio.sleep(1)
        with lock:
            r, a = pending_r[:], pending_a[:]
            pending_r.clear()
            pending_a.clear()
        rate_hist.append(len(r))
        totals["rate"] = round(sum(rate_hist) / len(rate_hist))
        if not clients:
            continue
        msg = json.dumps({"type": "tick", "readings": r, "alerts": a, "stats": stats()})
        for ws in list(clients):
            try:
                await ws.send_text(msg)
            except Exception:
                clients.discard(ws)


# ---------- historical analysis (from the parallel batch output) ----------
def load_hist():
    global HIST, TRENDS
    p = ROOT / "data" / "processed" / "station_day_clean.parquet"
    if not p.exists():
        print("[hist] run batch/parallel_pipeline.py first")
        return
    df = pd.read_parquet(p)
    df["City"] = df["City"].fillna("Unknown")
    df["StationName"] = df["StationName"].fillna(df["StationId"])
    df = df.dropna(subset=["AQI_calc"]).copy()
    df["month"] = df["Date"].dt.to_period("M").dt.to_timestamp()
    HIST = df
    try:
        import pymannkendall as mk
        for city, d in df.groupby("City"):
            s = d.groupby("month")["AQI_calc"].mean()
            if len(s) < 24:
                continue
            r = mk.original_test(s.values)
            TRENDS.append({"city": city, "trend": r.trend, "slope_per_year": round(float(r.slope) * 12, 1),
                           "p": round(float(r.p), 4), "mean_aqi": round(float(s.mean()))})
        TRENDS.sort(key=lambda x: -x["mean_aqi"])
    except ImportError:
        pass


@asynccontextmanager
async def lifespan(app):
    load_hist()
    threading.Thread(target=consume, daemon=True).start()
    threading.Thread(target=db_thread, daemon=True).start()
    task = asyncio.create_task(broadcaster())
    yield
    task.cancel()


app = FastAPI(title="Air Quality PDC", lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(ROOT / "dashboard" / "index.html")


@app.get("/api/stats")
def api_stats():
    return stats()


@app.get("/api/live")
def api_live():
    with lock:
        return sorted(live.values(), key=lambda v: -v["aqi"])


@app.get("/api/alerts")
def api_alerts(limit: int = 50):
    with lock:
        return list(alerts)[:limit]


@app.get("/api/history/cities")
def hist_cities():
    return sorted(HIST["City"].unique().tolist())


@app.get("/api/history/city/{city}")
def hist_city(city: str):
    m = HIST[HIST["City"] == city].groupby("month")[["AQI_calc"] + POLLS].mean().round(1)
    clean = lambda s: [None if pd.isna(v) else float(v) for v in s]
    return {"months": [x.strftime("%Y-%m") for x in m.index], "aqi": clean(m["AQI_calc"]),
            "pollutants": {p: clean(m[p]) for p in POLLS}}


@app.get("/api/history/seasonal")
def hist_seasonal():
    t = HIST.assign(m=HIST["Date"].dt.month).groupby(["City", "m"])["AQI_calc"].mean().unstack()
    t = t.loc[t.mean(axis=1).sort_values(ascending=False).index]
    data = []
    for ci, city in enumerate(t.index):
        for mi, m in enumerate(range(1, 13)):
            v = t.loc[city].get(m)
            if v is not None and pd.notna(v):
                data.append([mi, ci, round(float(v))])
    return {"cities": list(t.index), "data": data}


@app.get("/api/history/trends")
def hist_trends():
    return TRENDS


@app.get("/api/history/leaderboard")
def hist_leaderboard(n: int = 15):
    g = HIST.groupby(["StationName", "City"]).agg(
        mean_aqi=("AQI_calc", "mean"),
        pct_vp=("AQI_calc", lambda s: float((s > 300).mean() * 100))).reset_index()
    g = g.sort_values("mean_aqi", ascending=False).head(n)
    return [{"station": r.StationName, "city": r.City, "aqi": round(r.mean_aqi, 1),
             "pct_very_poor": round(r.pct_vp, 1)} for r in g.itertuples()]


@app.get("/api/benchmark")
def api_benchmark():
    p = ROOT / "benchmarks" / "results.json"
    if not p.exists():
        return JSONResponse({"error": "run benchmarks/run_benchmark.py first"}, status_code=404)
    return json.loads(p.read_text())


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)