# NVIDIA GPU Manufacturing Data Pipeline

> **End-to-End Data Engineering Platform for Semiconductor Manufacturing Telemetry**

A production-grade data engineering demonstration showcasing real-time telemetry generation, anomaly detection, and analytics for GPU wafer manufacturing processes. Built to demonstrate data pipeline architectures at NVIDIA scale.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Data Quality Validation](#data-quality-validation)
- [Data Generation Modes](#data-generation-modes)
- [Statistical Distributions](#statistical-distributions)
- [Anomaly Injection](#anomaly-injection)
- [Docker Usage](#docker-usage)
- [CLI Reference](#cli-reference)
- [Project Structure](#project-structure)
- [Technology Decisions](#technology-decisions)
- [Roadmap](#roadmap)

---

## Overview

This platform simulates a complete semiconductor manufacturing data pipeline, generating realistic telemetry data from GPU wafer fabrication processes. It demonstrates:

- **Realistic statistical modeling** using Normal, Poisson, and Beta distributions
- **Scalable batch processing** for datasets up to 10M+ records
- **Real-time streaming simulation** for continuous telemetry
- **Intelligent anomaly injection** simulating equipment failures
- **Containerized deployment** with Docker
- **Production-grade data quality validation** with dual validator strategy

### Use Cases

- Data engineering portfolio demonstration
- Machine learning model training datasets
- Anomaly detection algorithm testing
- Time-series analytics benchmarking
- Production pipeline prototyping

---

## Architecture

```
LOCAL ENVIRONMENT (Docker Compose)
┌─────────────────────────────────────────────────────────────┐
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
│  │ SILVER LAYER (Curated Data - Transformations)         │ │
│  │  • Deduplication (window functions)                    │ │
│  │  • Derived columns (shift, equipment age, hour)       │ │
│  │  • Quality gates (native PySpark validation)          │ │
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
- Dual validation strategy (Great Expectations + native PySpark)

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

## Data Quality Validation

This project implements **dual validation strategies** to support different compute environments—a production pattern used in multi-cloud architectures.

### Validation Architecture

```
validation/
├── __init__.py                    # Module exports
├── local_validator.py             # Great Expectations (local Docker)
└── pyspark_validator.py           # Native PySpark (Databricks Serverless)
```

### Local Development (Docker Environment)

**Use Case**: Development, testing, rich validation reports

**Implementation**: Great Expectations library with `SparkDFDataset`

```python
from validation.local_validator import LocalValidator

validator = LocalValidator()
results = validator.validate_dataframe(df, layer="bronze")

if not validator.is_valid(results):
    raise Exception(f"Validation failed: {results['critical_failures']} critical failures")
```

**Features:**
- Pre-built expectation suite from `great_expectations_config.py`
- Detailed validation reports with pass/fail metrics
- Works on local Spark clusters (supports PERSIST operations)
- Rich debugging output for development

### Databricks Serverless (Cloud Environment)

**Use Case**: Production pipelines, serverless compute, minimal dependencies

**Implementation**: Native PySpark validation queries

```python
from validation.pyspark_validator import PySparkValidator

validator = PySparkValidator()
results = validator.validate_dataframe(df, layer="silver")

if not validator.is_valid(results):
    raise Exception(f"Pipeline blocked: {results['critical_failures']} critical validations failed")
```

**Features:**
- No external library dependencies
- Compiles to optimized Spark SQL queries
- Compatible with Databricks Serverless compute (no PERSIST)
- Distributed validation at petabyte scale

### Shared Validation Rules (CVD Process Specs)

Both validators enforce the same manufacturing standards:

| Parameter | Normal Range | Optimal Target | Critical Threshold |
|-----------|--------------|----------------|-------------------|
| **Temperature** | 340-360°C | 350°C | 330-380°C (98% tolerance) |
| **Pressure** | 8-12 Torr | 10 Torr | 6-14 Torr (98% tolerance) |
| **Yield Rate** | 85-100% | 95%+ | 50-100% (95% tolerance) |
| **Defect Count** | 0-8 defects | ≤2 defects | 0-30 defects (98% tolerance) |

### Validation Result Structure

Both validators return identical result structures:

```python
{
    "total_validations": 16,
    "passed": 14,
    "failed": 2,
    "critical_failures": 0,  # Non-zero blocks pipeline
    "results": [
        {
            "validation": "wafer_id_uniqueness",
            "column": "wafer_id",
            "success": True,
            "critical": True
        },
        # ... more validation results
    ]
}
```

### When to Use Each Validator

**Local Docker Development:**
```bash
# Run with Great Expectations
python -c "
from pyspark.sql import SparkSession
from validation.local_validator import LocalValidator

spark = SparkSession.builder.appName('local-validation').getOrCreate()
df = spark.read.parquet('output/wafer-telemetry/*.parquet')

validator = LocalValidator()
results = validator.validate_dataframe(df, layer='bronze')
print(f'Pass rate: {results[\"passed\"]}/{results[\"total_validations\"]}')
"
```

**Databricks Notebooks:**
```python
# Cell: Validate Silver Layer
from validation.pyspark_validator import PySparkValidator

silver_df = spark.table("silver.wafer_telemetry")

validator = PySparkValidator()
results = validator.validate_dataframe(silver_df, layer="silver")

if not validator.is_valid(results):
    dbutils.notebook.exit(f"Validation failed: {results['critical_failures']} critical failures")
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
├── data-generator/                    # Production data pipeline
│   ├── generate_data_enhanced.py      # Synthetic data generator (batch/large/stream)
│   ├── kafka_producer.py              # Streams telemetry to Kafka
│   ├── kafka_consumer.py              # Consumes from Kafka, writes to MinIO
│   ├── explore_parquet.py             # Parquet file analysis utility
│   ├── great_expectations_config.py   # Validation rule definitions (CVD specs)
│   ├── requirements.txt               # Python dependencies
│   ├── Dockerfile                     # Container image definition
│   │
│   ├── validation/                    # Data quality validation module
│   │   ├── __init__.py                # Module exports
│   │   ├── local_validator.py         # Great Expectations wrapper (local)
│   │   └── pyspark_validator.py       # Native PySpark validator (Databricks)
│   │
│   ├── research/                      # Exploratory analysis
│   │   └── explore_distributions.py   # Statistical distribution verification
│   │
│   ├── output/                        # Generated data artifacts
│   │   ├── wafer_data.json            # Default output file
│   │   ├── generator.log              # Execution logs
│   │   └── wafer-telemetry/           # Parquet files for Databricks upload
│   │
│   └── images/                        # Documentation assets
│
├── databricks-notebooks/              # Cloud analytics notebooks
│   ├── 01_Bronze_Layer_Setup.py       # Raw data ingestion (Delta Lake)
│   ├── 02_Silver_Layer_Transformations.py  # Data cleaning and enrichment
│   └── 03_Data_Quality_Validation.py  # Native PySpark validation suite
│
├── docker-compose.yml                 # Multi-service orchestration (Kafka, MinIO)
└── README.md                          # This file
```

### Key Files Explained

**Core Generation:**
- `generate_data_enhanced.py`: Synthetic wafer telemetry generator with 3 modes (batch/large/stream)
- `great_expectations_config.py`: CVD process validation rules (temperature, pressure, yield thresholds)

**Streaming Pipeline:**
- `kafka_producer.py`: Produces telemetry to Kafka topics with equipment-based partitioning
- `kafka_consumer.py`: Consumes from Kafka, batches to Parquet, writes to MinIO (S3-compatible)

**Data Quality:**
- `validation/local_validator.py`: Great Expectations wrapper for local Docker environments
- `validation/pyspark_validator.py`: Native PySpark validation for Databricks Serverless
- Both share validation rules from `great_expectations_config.py`

**Databricks Notebooks:**
- `01_Bronze_Layer_Setup.py`: Schema enforcement, quality checks, Delta Lake ACID transactions
- `02_Silver_Layer_Transformations.py`: Deduplication, derived columns, quality gates
- `03_Data_Quality_Validation.py`: Automated validation suite with PySpark

**Infrastructure:**
- `docker-compose.yml`: Orchestrates Kafka, MinIO, and producer/consumer services
- `Dockerfile`: Container image for data generator with Python dependencies

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
| **Bound volumes** | Development flexibility, dynamic changes | **Cloud persistent volumes** (AWS EBS, Azure Disk, GCP Persistent Disk) |
| **Great Expectations** | Rich validation reports, local debugging | **Native PySpark** (Databricks Serverless compatibility, no library overhead) |

### Why These Choices Matter

**Local Development:**
- Zero cloud costs during development
- Full stack runs on laptop (no internet required)
- Easy debugging and troubleshooting
- Great Expectations provides rich validation reports

**Production Migration Path:**
- Each component has a clear cloud-native alternative
- Architecture patterns remain the same (Kafka → Kafka, S3 → S3)
- Minimal code changes needed for cloud deployment
- Skills transfer directly to enterprise environments
- Dual validation strategy supports multi-cloud deployments

### When to Upgrade

| Component | Upgrade Trigger | Why |
|-----------|----------------|-----|
| **Docker Compose → Kubernetes** | Need auto-scaling, multi-region, or >10 services | K8s provides orchestration, health checks, rolling updates |
| **MinIO → Cloud Storage** | Need 99.999% durability, global CDN, or >10TB data | Cloud providers offer built-in replication and disaster recovery |
| **Self-hosted Kafka → Managed** | Team lacks Kafka ops expertise or need 24/7 uptime | Managed services handle maintenance, upgrades, monitoring |
| **JSON → Avro** | Storage costs exceed $100/month or schema changes break consumers | Avro reduces storage 3x and provides backward compatibility |
| **Great Expectations → PySpark** | Deploying to Databricks Serverless or need petabyte scale | Native PySpark compiles to optimized Spark SQL, no PERSIST overhead |

---

## Roadmap

### Phase 1: Core Data Generation (Completed)
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
- [x] Dual validation strategy (Great Expectations + native PySpark)
- [ ] Airflow DAG orchestration (Bronze → Silver automation)
- [ ] Gold layer (aggregated KPIs, ML features)
- [ ] Time-series database storage (InfluxDB)
- [ ] Schema validation (Avro/Protobuf)

### Phase 3: Analytics Layer
- [ ] Real-time dashboards (Grafana)
- [ ] Anomaly detection ML models (Isolation Forest, LSTM)
- [ ] Statistical process control (SPC) charts
- [ ] Alerting system (PagerDuty/Slack)
- [ ] Integration with Databricks ML runtime

### Phase 4: Cloud Deployment
- [ ] Kubernetes deployment (NVIDIA GPU Operator)
- [ ] Cloud storage integration (S3/GCS/Azure)
- [ ] Distributed processing (Dask/Ray)
- [ ] Multi-region data replication
- [ ] Managed Kafka migration (Confluent Cloud)

### Phase 5: Advanced Features
- [ ] Digital twin simulation
- [ ] Predictive maintenance models
- [ ] Equipment performance optimization
- [ ] Supply chain integration
- [ ] RAPIDS/cuDF GPU acceleration

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

### Validation Output

```
VALIDATION SUMMARY
======================================================================
Total Validations: 16
Passed: 16 (100.0%)
Failed: 0
======================================================================

All validations passed!
Pipeline may continue to Gold layer
```

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

# Test validation module
python -c "
from validation.local_validator import LocalValidator
print('Validation module loaded successfully')
"
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
- **Great Expectations**: Data quality validation framework

---

## Questions?

For questions or collaboration opportunities, please open an issue or reach out via [kordelle12@gmail.com](mailto:kordelle12@gmail.com).

---

**If this project helped you, consider giving it a star!**