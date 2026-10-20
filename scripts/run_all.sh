#!/usr/bin/env bash
cd "$(dirname "$0")/.."
source .venv/bin/activate
mkdir -p logs

docker compose up -d
echo "Waiting for Kafka..."
until docker exec aq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
python streaming/create_topics.py

pids=()
for i in 1 2 3 4; do
  python streaming/worker.py --id w$i > logs/w$i.log 2>&1 &
  pids+=($!)
done
sleep 3
python simulator/producer.py > logs/producer.log 2>&1 &
pids+=($!)
python -m uvicorn main:app --app-dir api --port 8000 > logs/api.log 2>&1 &
pids+=($!)

trap 'echo; echo "Stopping..."; kill "${pids[@]}" 2>/dev/null; exit' INT TERM
echo "Running. Dashboard: http://localhost:8000   (Ctrl+C stops everything)"
wait