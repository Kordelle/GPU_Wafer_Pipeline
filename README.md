# NVIDIA GPU Manufacturing Data Pipeline

> **End-to-End Data Engineering Platform for Semiconductor Manufacturing Telemetry**

A production-grade data engineering demonstration showcasing real-time telemetry generation, anomaly detection, and analytics for GPU wafer manufacturing processes. Built to demonstrate data pipeline architectures at NVIDIA scale.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Data Generation Modes](#data-generation-modes)
- [Statistical Distributions](#statistical-distributions)
- [Anomaly Injection](#anomaly-injection)
- [Docker Usage](#docker-usage)
- [CLI Reference](#cli-reference)
- [Project Structure](#project-structure)
- [Roadmap](#roadmap)

---

## Overview

This platform simulates a complete semiconductor manufacturing data pipeline, generating realistic telemetry data from GPU wafer fabrication processes. It demonstrates:

- **Realistic statistical modeling** using Normal, Poisson, and Beta distributions
- **Scalable batch processing** for datasets up to 10M+ records
- **Real-time streaming simulation** for continuous telemetry
- **Intelligent anomaly injection** simulating equipment failures
- **Containerized deployment** with Docker

### Use Cases

- Data engineering portfolio demonstration
- Machine learning model training datasets
- Anomaly detection algorithm testing
- Time-series analytics benchmarking
- Production pipeline prototyping

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              LOCAL ENVIRONMENT (Docker Compose)              │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │    Kafka     │───▶│   Consumer   │───▶│    MinIO     │  │
│  │  (Streaming) │    │  (Batching)  │    │  (S3-like)   │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         ▲                                         │          │
│         │                                         │          │
│  ┌──────────────┐                                │          │
│  │   Producer   │                                │          │
│  │ (Synthetic   │                                │          │
│  │    Data)     │                                │          │
│  └──────────────┘                                │          │
└───────────────────────────────────────────────────┼─────────┘
                                                    │
                    Manual Upload (Parquet files)  │
                                                    ▼
┌─────────────────────────────────────────────────────────────┐
│                   DATABRICKS (Cloud Analytics)               │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ BRONZE LAYER (Raw Data - System of Record)            │ │
│  │  • Schema enforcement (explicit types)                 │ │
│  │  • Data quality checks (nulls, duplicates, ranges)    │ │
│  │  • Delta Lake (ACID transactions, time travel)        │ │
│  │  • Date partitioning (query pruning)                  │ │
│  └─────────────────────┬──────────────────────────────────┘ │
│                        │                                     │
│                        ▼                                     │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ SILVER LAYER (Curated Data - Coming Soon)             │ │
│  │  • Deduplication (window functions)                    │ │
│  │  • Derived columns (shift, equipment age, hour)       │ │
│  │  • Great Expectations (quality gates)                 │ │
│  │  • Delta Lake merge (incremental updates)             │ │
│  └─────────────────────┬──────────────────────────────────┘ │
│                        │                                     │
│                        ▼                                     │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ GOLD LAYER (Business Metrics - Future)                │ │
│  │  • Aggregated KPIs (yield by shift, defect trends)    │ │
│  │  • ML features (rolling averages, anomaly scores)     │ │
│  │  • BI-ready tables (Power BI, Tableau)                │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

**Medallion Architecture (Production Pattern):**
- **Bronze**: Raw data preservation, minimal transformations
- **Silver**: Cleaned, deduplicated, enriched data
- **Gold**: Business-level aggregations, ML features, dashboards


## 🏭 Databricks Integration (Bronze Layer)

### What is the Bronze Layer?

The **Bronze layer** is the foundation of the Medallion Architecture—it preserves raw data exactly as received from the Kafka consumer, with minimal transformations. Think of it as your **system of record** for manufacturing telemetry.

**Key Characteristics:**
- **Schema enforcement**: Explicit types catch producer bugs
- **Data quality checks**: Flag issues without dropping records
- **Immutability**: Never delete Bronze data (audit trail)
- **Date partitioning**: Efficient time-series queries

---

## Features

### Realistic Data Generation

- **Temperature Control**: Normal distribution (μ=350°C, σ=5°C)
- **Pressure Monitoring**: Normal distribution (μ=10 Torr, σ=0.5 Torr)
- **Yield Rate**: Beta distribution (α=20, β=2) → [0.80, 0.99]
- **Defect Count**: Poisson distribution (λ=2)

### Anomaly Injection (1% default rate)

| Anomaly Type | Description | Parameters |
|-------------|-------------|------------|
| **Temperature Spike** | Equipment overheating | +15-25°C above normal |
| **Temperature Drop** | Cooling system failure | -15-25°C below normal |
| **Pressure Fluctuation** | Vacuum leak detected | ±3-5 Torr deviation |
| **Defect Burst** | Contamination event | 10-30 defects, yield drops to 50-70% |

### Three Generation Modes

1. **Batch Mode**: Quick generation (1K-100K records)
2. **Large Mode**: Scalable generation with batching (10M+ records)
3. **Stream Mode**: Real-time continuous telemetry

### Production-Ready Features

- Docker containerization
- Memory-efficient batch processing
- Comprehensive logging
- CLI argument configuration
- Statistical validation tools

---

## Quick Start

### Prerequisites

- Docker installed
- 2GB+ free disk space (for large datasets)

### 1. Build the Docker Image

```bash
cd data-generator
docker build -t nvidia-data-pipeline:latest .
```

### 2. Generate Sample Data (Batch Mode)

```bash
docker run -v ${PWD}/output:/output nvidia-data-pipeline:latest --mode batch --records 1000
```

### 3. View Generated Data

```bash
cat output/wafer_data.json | head -5
```

### 4. Check Logs

```bash
cat output/generator.log
```

---

## Data Generation Modes

### Batch Mode (Default)

Generate a fixed number of records in memory:

```bash
docker run -v ${PWD}/output:/output nvidia-data-pipeline:latest \
  --mode batch \
  --records 10000 \
  --anomaly-rate 0.02 \
  --output /output/wafer_batch.json
```

**Output:**
- Single JSON Lines file
- Full dataset in memory
- Best for: < 1M records

### Large Mode (Scalable)

Generate massive datasets with memory-efficient batching:

```bash
docker run -v ${PWD}/output:/output nvidia-data-pipeline:latest \
  --mode large \
  --records 10000000 \
  --batch-size 1000000 \
  --output /output/wafer_large.json
```

**Output:**
- Processes in 1M record batches
- Appends to file incrementally
- Best for: 1M-100M+ records

**Performance**: ~50,000-100,000 records/second

### Stream Mode (Real-time)

Simulate continuous manufacturing telemetry:

```bash
docker run -v ${PWD}/output:/output nvidia-data-pipeline:latest \
  --mode stream \
  --interval 0.1 \
  --output /output/wafer_stream.json
```

**Output:**
- 1 record every 0.1 seconds (10 Hz)
- Appends continuously
- Press `Ctrl+C` to stop

---

## Statistical Distributions

### Temperature Distribution (Normal)

```
        │
  300   │         ┌─┐
  250   │       ┌─┘ └─┐
  200   │     ┌─┘     └─┐
  150   │   ┌─┘         └─┐
  100   │ ┌─┘             └─┐
   50   │─┘                 └─
        └─────────────────────────
       335  345  350  355  365
            Temperature (°C)
```

**Parameters:**
- Mean (μ): 350°C
- Std Dev (σ): 5°C
- 68% range: [345°C, 355°C]
- 95% range: [340°C, 360°C]

### Defect Count Distribution (Poisson)

```
  30%  │     █
  25%  │   █ █
  20%  │   █ █ █
  15%  │   █ █ █
  10%  │ █ █ █ █ █
   5%  │ █ █ █ █ █ █
       └─────────────────
        0 1 2 3 4 5 6
         Defect Count
```

**Parameters:**
- Lambda (λ): 2 defects/wafer
- Discrete (integers only)
- Right-skewed distribution

---

## Data Schema

### Record Structure

```json
{
  "timestamp": "2024-10-24T15:30:45.123456",
  "wafer_id": "W00000042",
  "equipment_id": "EQ003",
  "temperature_c": 348.23,
  "pressure_torr": 10.15,
  "yield_rate": 0.9542,
  "defect_count": 2,
  "is_anomaly": false
}
```

### Field Descriptions

| Field | Type | Description | Range |
|-------|------|-------------|-------|
| `timestamp` | datetime | Record generation time | ISO 8601 format |
| `wafer_id` | string | Unique wafer identifier | W00000000 - W99999999 |
| `equipment_id` | string | Manufacturing equipment | EQ001 - EQ004 |
| `temperature_c` | float | Process temperature | 330-370°C (normal), wider for anomalies |
| `pressure_torr` | float | Chamber pressure | 8-12 Torr (normal), wider for anomalies |
| `yield_rate` | float | Wafer yield percentage | 0.80-0.99 (normal), 0.50-0.70 (anomalies) |
| `defect_count` | int | Number of defects | 0-8 (normal), 10-30 (anomalies) |
| `is_anomaly` | bool | Anomaly flag | true/false |

---

## Docker Usage

### Build Custom Image 

```bash
docker build -t nvidia-data-pipeline:v1.0 .
```

### Run with Custom Configuration

```bash
# Windows PowerShell
docker run -v ${PWD}/output:/output nvidia-data-pipeline:latest `
  --mode large `
  --records 5000000 `
  --batch-size 500000 `
  --anomaly-rate 0.015 `
  --seed 12345

# Linux/macOS
docker run -v $(pwd)/output:/output nvidia-data-pipeline:latest \
  --mode large \
  --records 5000000 \
  --batch-size 500000 \
  --anomaly-rate 0.015 \
  --seed 12345
```

### Interactive Shell

```bash
docker run -it -v ${PWD}/output:/output nvidia-data-pipeline:latest /bin/bash
```

---

## CLI Reference

```
usage: generate_data_enhanced.py [-h] [--mode {batch,large,stream}]
                                 [--records RECORDS]
                                 [--batch-size BATCH_SIZE]
                                 [--interval INTERVAL] [--output OUTPUT]
                                 [--anomaly-rate ANOMALY_RATE] [--seed SEED]

Generate synthetic semiconductor manufacturing data

optional arguments:
  -h, --help            show this help message and exit
  --mode {batch,large,stream}
                        Generation mode (default: batch)
  --records RECORDS     Number of records for batch/large mode (default: 1000)
  --batch-size BATCH_SIZE
                        Batch size for large mode (default: 1000000)
  --interval INTERVAL   Seconds between records in stream mode (default: 1.0)
  --output OUTPUT       Output file path (default: /output/wafer_data.json)
  --anomaly-rate ANOMALY_RATE
                        Fraction of anomalous records (default: 0.01 = 1%)
  --seed SEED           Random seed for reproducibility (default: 42)
```

---

## Project Structure

```
Semiconductor-Telemetry-Platform/
├── data-generator/          # Production streaming pipeline
│   ├── kafka_producer.py    # Generates wafer telemetry
│   ├── kafka_consumer.py    # Writes to MinIO
│   └── generate_data_enhanced.py
│
├── notebooks/               # Research & analysis
│   └── explore_distributions.py
│
└── docker-compose.yml       # Infrastructure definition
```

## Roadmap

### Phase 1: Core Data Generation ✅
- [x] Statistical distribution implementation
- [x] Anomaly injection engine
- [x] Batch/Large/Stream modes
- [x] Docker containerization
- [x] CLI interface

### Phase 2: Data Pipeline (In Progress)
- [x] Kafka producer (streaming telemetry)
- [x] Kafka consumer (archival to MinIO as Parquet)
- [x] Databricks-compatible schema (microsecond timestamps)
- [x] Bronze layer in Databricks (Delta Lake + quality checks)
- [x] Manual Parquet upload workflow (MinIO → Databricks)
- [x] Silver transformations (deduplication, enrichment, quality gates)
- [ ] Great Expectations validation suite
- [ ] Airflow DAG orchestration (Bronze → Silver automation)
- [ ] Gold layer (aggregated KPIs, ML features)
- [ ] Time-series database storage (InfluxDB)
- [ ] Data quality monitoring
- [ ] Schema validation (Avro/Protobuf)

### Phase 3: Analytics Layer
- [ ] Real-time dashboards (Grafana)
- [ ] Anomaly detection ML models (Isolation Forest, LSTM)
- [ ] Statistical process control (SPC) charts
- [ ] Alerting system (PagerDuty/Slack)

### Phase 4: Cloud Deployment
- [ ] Kubernetes deployment (NVIDIA GPU Operator)
- [ ] Cloud storage integration (S3/GCS/Azure)
- [ ] Distributed processing (Dask/Ray)
- [ ] Multi-region data replication

### Phase 5: Advanced Features
- [ ] Digital twin simulation
- [ ] Predictive maintenance models
- [ ] Equipment performance optimization
- [ ] Supply chain integration

---

## Example Output

### Sample Records

```json
{"timestamp":"2024-10-24T15:30:40.261","wafer_id":"W00000000","equipment_id":"EQ003","temperature_c":347.25,"pressure_torr":10.26,"yield_rate":0.9598,"defect_count":2,"is_anomaly":false}
{"timestamp":"2024-10-24T15:30:40.368","wafer_id":"W00000001","equipment_id":"EQ003","temperature_c":347.65,"pressure_torr":10.27,"yield_rate":0.9785,"defect_count":1,"is_anomaly":false}
{"timestamp":"2024-10-24T15:30:40.473","wafer_id":"W00000002","equipment_id":"EQ003","temperature_c":371.34,"pressure_torr":9.94,"yield_rate":0.6542,"defect_count":18,"is_anomaly":true}
```

### Log Output

```
2024-10-24 15:30:40,123 - __main__ - INFO - Initialized generator with 4 equipment IDs
2024-10-24 15:30:40,124 - __main__ - INFO - Temperature: μ=350°C, σ=5°C
2024-10-24 15:30:40,125 - __main__ - INFO - Anomaly rate: 1.0%
2024-10-24 15:30:40,126 - __main__ - INFO - Starting large dataset generation: 10,000,000 records
2024-10-24 15:30:40,127 - __main__ - INFO - Batch size: 1,000,000 | Batches: 10
2024-10-24 15:30:52,341 - __main__ - INFO - Batch 1: 1,000,000 records | Progress: 10.0% | Rate: 81,300 rec/sec
2024-10-24 15:31:04,562 - __main__ - INFO - Batch 2: 1,000,000 records | Progress: 20.0% | Rate: 82,150 rec/sec
...
2024-10-24 15:32:18,789 - __main__ - INFO - Generated 10,000,000 records in 98.67s
2024-10-24 15:32:18,790 - __main__ - INFO -    Average rate: 101,347 records/sec
2024-10-24 15:32:18,791 - __main__ - INFO -    File size: 1,234.56 MB
```


---
## Technology Decisions

This project uses specific technologies for local development and learning purposes. Here's the rationale and production alternatives:

| Decision | Rationale | Production Alternative |
|----------|-----------|------------------------|
| **Docker Compose** | Local dev simplicity, easy multi-service orchestration | **Kubernetes** (AKS/EKS/GKE) for production-grade container orchestration |
| **MinIO** | S3-compatible API, zero cloud costs, local testing | **AWS S3**, **Azure Blob Storage**, **Google Cloud Storage** |
| **Kafka (self-hosted)** | Learn end-to-end orchestration, full control | **Confluent Cloud**, **AWS MSK**, **Azure Event Hubs** |
| **Manual offset commits** | Exactly-once delivery guarantees, data integrity | **Keep in production** (critical for preventing data loss/duplication) |
| **JSON serialization** | Human-readable debugging, easy inspection | **Avro** (3x compression, schema evolution, type safety) |
| **Binded Volumes** | Development, Dynamic | **Cloud persistent volumes** (AWS EBS, Azure Disk, GCP Persistent Disk) |

### Why These Choices Matter

**Local Development:**
- Zero cloud costs during development
- Full stack runs on laptop (no internet required)
- Easy debugging and troubleshooting

**Production Migration Path:**
- Each component has a clear cloud-native alternative
- Architecture patterns remain the same (Kafka → Kafka, S3 → S3)
- Minimal code changes needed for cloud deployment
- Skills transfer directly to enterprise environments

### When to Upgrade

| Component | Upgrade Trigger | Why |
|-----------|----------------|-----|
| **Docker Compose → Kubernetes** | Need auto-scaling, multi-region, or >10 services | K8s provides orchestration, health checks, rolling updates |
| **MinIO → Cloud Storage** | Need 99.999% durability, global CDN, or >10TB data | Cloud providers offer built-in replication and disaster recovery |
| **Self-hosted Kafka → Managed** | Team lacks Kafka ops expertise or need 24/7 uptime | Managed services handle maintenance, upgrades, monitoring |
| **JSON → Avro** | Storage costs exceed $100/month or schema changes break consumers | Avro reduces storage 3x and provides backward compatibility |

---

## Contributing

This is a portfolio project demonstrating data engineering capabilities. Feedback and suggestions welcome!

### Development Setup

```bash
# Clone repository
git clone https://github.com/Kordelle/Semiconductor-Telemetry-Platform.git
cd Semiconductor-Telemetry-Platform/data-generator

# Install dependencies
pip install -r requirements.txt

# Run locally (without Docker)
python generate_data_enhanced.py --mode batch --records 1000
```

---

## License

MIT License

Copyright (c) 2025 Kordelle

---

## Author

**Kordelle Walker**
- LinkedIn: [linkedin.com/in/kordelle-walker](https://www.linkedin.com/in/kordelle-walker-7382b9200/)
- GitHub: [@Kordelle](https://github.com/Kordelle)

---

## Acknowledgments

- **NVIDIA**: Inspiration from GPU manufacturing processes
- **Semiconductor Industry**: Real-world telemetry patterns
- **Open Source Community**: Python data science ecosystem

---

## Questions?

For questions or collaboration opportunities, please open an issue or reach out via [kordelle12@gmail.com](mailto:kordelle12@gmail.com).

---

**If this project helped you, consider giving it a star!**