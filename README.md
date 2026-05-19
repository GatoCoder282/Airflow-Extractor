# Airflow Extractor

Python service that pulls operational data from Airflow and publishes it to Redis Streams for downstream processing.

## Overview

Polls the Airflow API and PostgreSQL metadata database, tracks extraction state to avoid duplicate events, and publishes normalized events to `stream:airflow_events` and `stream:dag_catalog_sync`.

Architecture follows the hexagonal (Ports & Adapters) pattern:

```
extraction/
├── domain/        # Models and enums
├── ports/         # Interfaces (publisher, repositories)
├── adapters/      # Implementations (HTTP client, PostgreSQL, Redis, stdout)
├── services/      # Extraction, initialization, and state-tracking logic
├── infrastructure/# Dependency factory
└── extractor/     # Entry point and config
```

## Setup

1. Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. Create a `.env` file with your environment values (see required variables below):

```env
AIRFLOW_HOST=http://localhost:8080
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=your_password

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=airflow

REDIS_HOST=localhost
REDIS_PORT=6379
```

3. Run the extractor:

```bash
python -m extraction.extractor.main
```

## Dependencies

See [requirements.txt](requirements.txt).
