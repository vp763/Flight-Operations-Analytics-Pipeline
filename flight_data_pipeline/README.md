# Real-Time Flight Data Pipeline (Airflow + Snowflake)

A simple, automated data pipeline that extracts live flight data from the OpenSky Network API, loads it into Snowflake, and cleans/aggregates it using the Medallion architecture (Bronze -> Silver -> Gold).

I built this project to get hands-on experience handling semi-structured streaming/API data, automating workloads with Apache Airflow in Docker, and using Snowflake's native JSON support (`VARIANT`) for ELT transformations.

---

## What the Pipeline Does

1. **Extract (API):** Hits OpenSky Network's public REST API to fetch live aircraft transponder data (ICAO 24-bit address, callsign, coordinates, speed, and altitude).
2. **Bronze Layer (Raw Staging):** Stages the raw JSON file directly into Snowflake (`@BRONZE.FLIGHT_STAGE`) and loads it into a `VARIANT` column using `COPY INTO`.
3. **Silver Layer (Cleaning & Typing):** 
   - Converts nested JSON arrays into structured columns.
   - Cleans missing/blank callsigns as `'UNKNOWN'`.
   - Converts units: altitude (meters -> feet) and speed (m/s -> knots).
   - Deduplicates flight records using `QUALIFY ROW_NUMBER()`.
4. **Gold Layer (Analytics):** Aggregates country-level airspace stats (active flights, airborne vs. grounded count, average speeds) for easy reporting.

---

## Project Structure

```text
flight_data_pipeline/
├── dags/
│   └── flight_pipeline_dag.py     # Main Airflow DAG
├── sql/
│   └── setup.sql                  # Snowflake database, schemas & tables setup
├── data/
│   └── flights/                   # Local raw JSON storage (mounted in Docker)
├── Dockerfile                     # Custom Airflow image with Snowflake connector
├── docker-compose.yaml            # Airflow LocalExecutor setup
├── requirements.txt               # snowflake-connector-python, requests, pandas
├── .env.example
├── .gitignore
└── README.md
```

---

## Tech Stack

- **Orchestration:** Apache Airflow 2.10.2 (Docker)
- **Data Warehouse:** Snowflake (Free Trial / Standard Edition)
- **Language & Libraries:** Python 3.12, `snowflake-connector-python`, `requests`
- **Source API:** OpenSky Network (Free live flight states)

---

## Quickstart & Local Setup

### 1. Snowflake Initial Setup
Open your Snowflake worksheet and run the queries inside `sql/setup.sql`. This creates the `FLIGHT_DB` database, three schemas (`BRONZE`, `SILVER`, `GOLD`), and the required staging tables.

### 2. Setup Environment Variables
Clone the repo and create your `.env` file:

```bash
cp .env.example .env
```

Add your Snowflake account details:
```env
AIRFLOW_UID=50000
SNOWFLAKE_ACCOUNT=your_account_locator (e.g. xy12345.ap-south-1.aws)
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

### 3. Build & Run Airflow
```bash
# Build custom image with snowflake connector
docker compose build

# Start services
docker compose up -d
```

### 4. Trigger the DAG
- Open `http://localhost:8080` (login: `airflow` / `airflow`).
- Unpause and trigger `flight_tracker_etl`.
- Once completed, check Snowflake to verify data loaded into `SILVER.CLEAN_FLIGHTS` and `GOLD.AIRSPACE_TRAFFIC_METRICS`.

---

## What I Learned & Challenges Faced

- **Handling Unstructured Variant Data in Snowflake:** The OpenSky API returns states as an array of mixed types instead of named JSON objects. Querying them using zero-based indexing (`raw_data[0]`, `raw_data[1]`) inside Snowflake SQL showed me how flexible `VARIANT` columns are for schema-on-read.
- **Docker Volume Mounting:** Running Airflow in Docker while writing files locally required properly mapping `./data` between the host machine and container so the Snowflake `PUT` command could find the file path.
- **Cost Optimization in Snowflake:** Set warehouse auto-suspend to 60 seconds on an X-Small warehouse so credits aren't wasted between scheduled hourly runs.
