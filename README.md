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
Airflow (dbt post run checks)
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

* DAG: `dbt_monitoring.py`
* Schedule: every 4 hours at HH:30 (30 */4 * * *) — it starts 30 minutes after the ingestion DAG to ensure the full pipeline (ingestion → dbt) has finished.
* Purpose: Post-dbt data quality checks (row counts, freshness, max timestamp validations)
* Triggers: Runs according to its own schedule after dbt models are materialized
* Alerts: Sends email on failure
* Scope: Checks staging → intermediate → marts tables in BigQuery

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

### **Standard Tests**
   
   * `unique`
   * `not_null`
   * `accepted_values`
   * `relationships` (referential integrity)
   
### **Custom Tests**
   
   * `check_duplicates`
   * `check_rain_snow_logic`
   * `check_timestamps`
   * `check_tmp_wind_range`

* dbt job runs 15 minutes after ingestion DAG completion (scheduled at HH:16).

---

## **5. BigQuery Clean Layer**

* Dataset: `clean`
* Tables:

  * staging layer (normalized raw data)
  * intermediate layer (unified and enriched transformations)
  * marts layer (analytics-ready dimensional + fact structures)
  * metadata for data-quality monitoring

* Materialization: stg + int → views, marts + metadata → tables
* Staging, intermediate, and metadata views reside in the same clean dataset as marts to simplify process due to the small dataset size

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
* Quality controls: schema tests, referential checks, and custom validations
* Monitoring: scheduled DAG for post-dbt data quality checks with alerts on failure
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
* marts (fact/dim)
* Airflow dbt post run check

**Planned:**

* streaming simulation (Pub/Sub + Cloud Run): canceled due to free-tier limitations
* Looker dashboard
* CI/CD via GitHub Actions
