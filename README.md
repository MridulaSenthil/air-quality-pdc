# Air Quality Monitoring and Pollution Analysis
Parallel and Distributed Computing lab project.

Dataset: Air Quality Data in India (2015-2020), CPCB, via Kaggle (rohanrao/air-quality-data-in-india).
Download the CSVs into `data/raw/`.
## Architecture
Sensors (simulator) → Kafka (4 partitions, keyed by station) → 4 consumer-group workers (AQI + alerts)
→ FastAPI (WebSocket + REST) → Dashboard. Batch path: multiprocessing over per-station partitions.

## Run
1. Download the CSVs into data/raw/ (see dataset link above)
2. python batch/parallel_pipeline.py 4   # creates the cleaned data
3. python benchmarks/run_benchmark.py 30 # speedup results
4. ./scripts/run_all.sh                  # open http://localhost:8000