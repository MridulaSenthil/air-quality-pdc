# Air Quality Monitoring and Pollution Analysis
Parallel and Distributed Computing lab project (AirWatch dashboard).

Dataset: Air Quality Data in India (2015-2020), CPCB, via Kaggle (rohanrao/air-quality-data-in-india).

## Architecture
Sensors (simulator) → Kafka (4 partitions, keyed by station) → 4 consumer-group workers (AQI + alerts)
→ FastAPI (WebSocket + REST) → Dashboard.
Batch path: multiprocessing over per-station partitions.

## Requirements
- Docker Desktop (running)
- Python 3.11
- macOS or Linux (on Windows use WSL)

## Quick start
```bash
git clone <https://github.com/MridulaSenthil/air-quality-pdc.git>
cd air-quality-pdc
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./scripts/run_all.sh
```
Then open http://localhost:8000

Ports 9092 (Kafka), 5432 (Postgres) and 8000 (dashboard) must be free.
Press Ctrl+C in the same terminal to stop everything.

## Data
The processed dataset (data/processed/) and benchmark results are included,
so the dashboard runs without any download.

## Re-running the batch pipeline (optional)
Download the raw CSVs from Kaggle into data/raw/, then:
```bash
python batch/parallel_pipeline.py 4      # creates the cleaned data
python benchmarks/run_benchmark.py 30    # speedup results
```