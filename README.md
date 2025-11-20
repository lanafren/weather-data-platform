# **API → Airflow → GCS/BigQuery → dbt Pipeline**

End-to-end data pipeline on **GCP**, using **Airflow** for ingestion and **dbt** for transformations. The system loads weather data from OpenWeatherMap, stores raw payloads in GCS/BigQuery, and produces analytics-ready tables for BI.

---

## **1. Architecture Overview**

```
OWM API
   ↓
Airflow (batch ingestion)
   ↓
GCS Raw (bronze)
   ↓
BigQuery Raw
   ↓
dbt: stg → int → marts
   ↓
BigQuery Clean (silver/gold)
   ↓
Looker
```

---

## **2. Pipeline Components**

### **OpenWeatherMap API**

* Endpoints: `/weather`, `/forecast`
* JSON responses
* API key authentication
* All timestamps normalized to **UTC**

---

### **Airflow**

* DAG: `owm_batch_bq.py`
* Schedule: every 4 hours (`0 */4 * * *`)
* Retry logic enabled
* Idempotent writes to GCS and BigQuery
* Logical time aligned to 4-hour windows

---

### **GCS Raw (Bronze)**

* NDJSON files, 1 per ingestion run
* Naming: `current_YYYYMMDD_HH.ndjson`
* Retention: 30 days
* Contents: `{fetched_at, source, data}`

---

### **BigQuery Raw**

* Dataset: `raw`
* Schema:

  * `fetched_at TIMESTAMP`
  * `source STRING`
  * `data JSON`
* Daily partitioning on `fetched_at`
* Append-only, duplicate-safe

---

## **3. IAM & Security (High-Level)**

* Separate service accounts for Airflow and dbt
* Least-privilege access (BQ + GCS scoped per component)
* No secrets stored in the repository
* Local development uses environment variables (e.g., `GOOGLE_APPLICATION_CREDENTIALS`)

---

## **4. dbt Transformations**

### **Layers**

* **stg** — normalization, renaming, typing, timestamp cleanup
* **int** — unified structure for current + forecast data
* **marts** — curated fact/dimension tables

### **Tests**

* `unique`
* `not_null`
* `accepted_values`
* `relationships` (referential integrity)

---

## **5. BigQuery Clean Layer**

Planned curated outputs:

* `dim_time`
* `fact_weather_measurements`
* unified hourly/forecasted views

---

## **6. Looker**

(To be added)
Dashboards for temperature, humidity, precipitation, and forecast accuracy.

---

## **7. Project Scope**

This repository covers:

* Ingestion pipeline design (Airflow → GCS → BigQuery)
* Raw → staged → modeled ELT flow using dbt
* Data modeling: grain definition, unified schema, typed fields
* Quality controls: schema tests and referential checks
* Secure execution with isolated service accounts

---

## **8. Current Status**

**Completed:**

* Airflow batch ingestion
* GCS/BigQuery raw layers
* dbt project setup
* staging models + schema tests
* intermediate model
* unified weather record

**In progress:**

* marts (fact/dim)

**Planned:**

* streaming simulation (Pub/Sub + Cloud Run)
* Looker dashboard
* CI/CD via GitHub Actions
