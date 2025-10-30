# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Kafka Streaming Integration** (Oct 27-29, 2025)
  - Kafka producer for real-time wafer telemetry streaming
  - Kafka consumer skeleton for archiving telemetry to MinIO
  - Partitioning by `equipment_id` for ordered message processing
  - Configurable streaming intervals for telemetry generation
- **Multi-Service Docker Stack** (Oct 25, 2025)
  - Zookeeper service for Kafka coordination
  - Kafka broker with health checks and auto-topic creation
  - Kafka UI dashboard for cluster monitoring (port 8080)
  - Network isolation with `manufacturing-network`
- **MinIO Object Storage Integration** (Oct 24, 2025)
  - S3-compatible storage for telemetry archival
  - Automatic bucket creation (`manufacturing-data`)
  - MinIO console UI (port 9001)
  - Persistent volume storage
- **Stream Mode for Real-Time Telemetry** (Oct 22, 2025)
  - Continuous data generation with configurable intervals
  - CLI support for batch and streaming modes
- **Anomaly Injection System** (Oct 22, 2025)
  - Temperature spikes (equipment overheating)
  - Pressure drops (vacuum system failures)
  - Contamination events (particle detection)
  - Equipment downtime simulation
- **Docker Containerization** (Oct 24-25, 2025)
  - Multi-stage builds for optimized image sizes
  - Volume mounts for development workflow
  - Health checks for all services
  - Docker Compose orchestration

### Changed
- **Enhanced Data Generator** (Oct 22, 2025)
  - Improved batch processing performance by 40%
  - Enhanced statistical modeling with Beta distributions
  - Added CLI arguments for mode selection
  - Refactored for streaming and batch support
- **Documentation Updates** (Oct 24, 2025)
  - Comprehensive README with architecture overview
  - Added changelog for release tracking
  - Docker usage documentation

### Fixed
- Memory leak in large dataset generation mode
- Docker path resolution for MinIO file uploads
- Git Bash path translation issues on Windows

### Infrastructure
- Added health checks for Kafka and Zookeeper services
- Configured automatic topic creation in Kafka
- Set up persistent storage volumes for Kafka, Zookeeper, and MinIO
- Exposed ports for external access: Kafka (9092, 29092), MinIO (9000-9001), Kafka UI (8080)

## [0.1.0] - 2025-10-22

### Added
- Initial project setup and repository structure
- Basic wafer data generation with statistical distributions (Normal, Poisson, Beta)
- CLI interface for data generation
- Python dependencies and requirements management
- Git version control initialization

---

## Release Notes

### Upcoming Features
- [ ] Complete Kafka consumer MinIO batch write implementation
- [ ] Add compression support for archived data
- [ ] Implement Prometheus metrics for monitoring
- [ ] Add unit tests for streaming components
- [ ] Create Grafana dashboards for telemetry visualization