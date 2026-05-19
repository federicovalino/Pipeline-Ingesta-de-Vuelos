# Real-time Flight Ingestion Pipeline — PySpark & Delta Lake

End-to-end data pipeline for real-time flight tracking, built with PySpark and Delta Lake following a Medallion architecture (Bronze / Silver / Gold).

---

## Overview

This pipeline consumes live flight position data from the [OpenSky Network](https://opensky-network.org/) public API and builds a reliable, incremental, and enriched data source that can be consumed by Business Intelligence tools such as Power BI or Tableau, or by any operational system requiring up-to-date airspace information.

The pipeline answers questions such as:

- How many flights are currently active over a given airspace?
- Which countries do the aircraft originate from?
- Which aircraft are taking off, landing, or cruising?
- What manufacturer, model and operator does each identified aircraft belong to?

---

## Architecture

```
OpenSky Network API
        ↓
  Bronze (raw)     → Raw JSON persisted as-is in Delta Lake
        ↓
  Silver (clean)   → Correct types, readable timestamps, flight status classification
        ↓
  Gold (enriched)  → JOIN with static aircraft dataset → manufacturer, model, operator
```

---

## Project Structure

```
pipeline-flights/
├── src/
│   ├── spark_session.py       # SparkSession initialization with Delta Lake
│   ├── ingesta.py             # API consumption with retries and exponential backoff
│   ├── bronze.py              # Raw data persistence in Delta format
│   ├── silver.py              # Transformations, type casting, flight status logic
│   ├── gold.py                # JOIN with static dataset
│   └── checkpoint.py          # Incremental load state management
├── static/
│   └── aircraftDatabase.csv   # 199 aircraft sample (South America, Europe, USA)
├── data/
│   ├── bronze/flights/        # Delta table — Bronze layer
│   ├── silver/flights/        # Delta table — Silver layer
│   ├── gold/enriched_flights/ # Delta table — Gold layer
│   └── checkpoints/
│       └── state.json         # Last successful execution state
├── tests/
│   ├── test_duplicates.py
│   └── test_join.py
├── main.py                    # Pipeline entry point
├── requirements.txt
└── README.md
```

---

## Installation & Usage

### Requirements

- Python 3.11
- Java 17 (required by Spark)
- Hadoop winutils (Windows only)

### Install dependencies

```bash
py -3.11 -m pip install -r requirements.txt
```

### Run the pipeline

```bash
py -3.11 main.py
```

Each execution runs the full pipeline: API ingestion, Bronze persistence, Silver transformation, Gold enrichment and checkpoint update.

### Run the tests

> **Important:** tests assume Delta tables already exist in `data/`.
> Run the pipeline at least once before executing the test suite.

```bash
py -3.11 -m pytest tests/ -v
```

---

## Incremental Load & Checkpointing

Checkpointing is the core mechanism that ensures each pipeline run processes only new data since the last successful execution.

### Checkpoint structure

State is persisted in `data/checkpoints/state.json`:

```json
{
  "last_execution": "2026-05-03T22:12:40.134910+00:00",
  "records_processed": 12,
  "status": "success"
}
```

### Execution flow

```
1. Read checkpoint
        ↓
   Exists and status = "success"?
        ↓ Yes                       ↓ No
   Incremental load             Full load
   from last_execution          from scratch
        ↓
2. Call API (with retries)
        ↓
3. MERGE into Bronze  →  insert new records only
        ↓
4. MERGE into Silver  →  insert or update
        ↓
5. MERGE into Gold    →  insert or update
        ↓
6. Save checkpoint with status = "success"
   ← Only reached if the full pipeline completed without errors
```

### Design guarantees

**Idempotency** — the pipeline uses MERGE across all layers instead of append or overwrite. Running the same execution twice produces identical results — no duplicates.

**Late checkpoint** — the checkpoint is saved only after all layers complete successfully. If the pipeline fails at Silver or Gold, the checkpoint does not advance and the next run retries from the same point.

**First run** — if no checkpoint exists or the file is empty, the pipeline performs a full load with no date filter.

### Production evolution

In a production environment, the JSON checkpoint would be replaced by a dedicated Delta table, enabling full execution history, auditing and coordination across multiple pipelines.

---

## Error Handling

The ingestion layer implements retries with exponential backoff:

- Up to 3 retries on timeout or connection errors
- On HTTP 429 (rate limiting): waits 5s → 10s → 20s
- If all retries fail, the pipeline raises an exception and the checkpoint does not advance

---

## Silver Transformations

| Field | Transformation |
|---|---|
| `time_position` | Unix epoch (Long) → Readable timestamp |
| `last_contact` | Unix epoch (Long) → Readable timestamp |
| `timestamp_api` | Unix epoch (Long) → Readable timestamp |
| `baro_altitude` | Rounded to 2 decimal places |
| `velocity` | Rounded to 2 decimal places |
| `longitude / latitude` | Rounded to 4 decimal places |
| `callsign` | Null → "UNKNOWN" |
| `flight_status` | Classification: `in_flight` / `on_ground` / `taking_off_or_landing` |

---

## Schema Evolution

All layers are written with `mergeSchema: true`. If the API adds new fields in the future, the pipeline absorbs them automatically without breaking existing tables or requiring manual migrations.

---

## Gold Enrichment

The JOIN in Gold connects each flight to its aircraft using `icao24` — the unique physical identifier of the aircraft, equivalent to a license plate.

**Static source:** `aircraftDatabase.csv` — a sample of 199 aircraft from airlines in South America, Europe and USA (LATAM, Flybondi, Iberia, Air Europa, KLM, Delta, American, among others).

**In production**, the full OpenSky database (~520k records) would be used, significantly increasing the JOIN match rate. The pipeline logic is independent of dataset size.

---

## Tests

| Test | Description |
|---|---|
| `test_bronze_no_duplicates` | `icao24 + timestamp_api` is unique in Bronze |
| `test_silver_no_duplicates` | `icao24 + timestamp_api` is unique in Silver |
| `test_gold_no_duplicates` | `icao24 + timestamp_api` is unique in Gold |
| `test_silver_has_records` | Silver is not empty after pipeline run |
| `test_flight_status_valid_values` | `flight_status` only contains expected values |
| `test_gold_matches_silver_count` | JOIN preserves row count from Silver |
| `test_gold_has_enrichment_columns` | Gold contains columns from the JOIN |
| `test_join_no_nulls_on_key_fields` | Silver fields are not null in Gold |
| `test_gold_icao24_subset_of_silver` | Gold contains no `icao24` values absent from Silver |

---

## Technical Decisions

**Why OpenSky?** Free public API, no registration required, real flight data that changes constantly — ideal for demonstrating incremental ingestion.

**Why JSON for the checkpoint?** Sufficient for this scope and easy to audit. In production it would be migrated to a dedicated Delta table.

**Why no Docker?** The focus of this project is the data pipeline itself. In production it would be containerized using a `bitnami/spark` base image or deployed on Databricks.