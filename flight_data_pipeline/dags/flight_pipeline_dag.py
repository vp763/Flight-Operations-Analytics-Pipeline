import os
import json
import requests
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import snowflake.connector

DATA_DIR = "/opt/airflow/data/flights"
os.makedirs(DATA_DIR, exist_ok=True)

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="FLIGHT_DB",
        schema="BRONZE",
        role="ACCOUNTADMIN"
    )

def extract_flights(**kwargs):
    url = "https://opensky-network.org/api/states/all"
    res = requests.get(url, timeout=30)
    data = res.json().get("states", [])
    
    valid_flights = [f for f in data if f[5] is not None and f[6] is not None][:300]
    
    file_name = f"flights_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    file_path = os.path.join(DATA_DIR, file_name)
    
    with open(file_path, "w") as f:
        json.dump(valid_flights, f)
        
    print(f"Saved {len(valid_flights)} flights to {file_path}")
    kwargs["ti"].xcom_push(key="flight_file", value=file_path)

def load_bronze(**kwargs):
    file_path = kwargs["ti"].xcom_pull(task_ids="extract_flights", key="flight_file")
    file_name = os.path.basename(file_path)
    
    conn = get_snowflake_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(f"PUT file://{file_path} @BRONZE.FLIGHT_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")
        cur.execute(f"""
            COPY INTO BRONZE.RAW_FLIGHTS (raw_data, file_name)
            FROM (SELECT $1, '{file_name}' FROM @BRONZE.FLIGHT_STAGE/{file_name}.gz)
            FILE_FORMAT = (TYPE = 'JSON');
        """)
        print("Bronze load complete.")
    finally:
        cur.close()
        conn.close()

def transform_silver(**kwargs):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OVERWRITE INTO SILVER.CLEAN_FLIGHTS
            SELECT
                raw_data[0]::STRING AS icao24,
                COALESCE(NULLIF(TRIM(raw_data[1]::STRING), ''), 'UNKNOWN') AS callsign,
                raw_data[2]::STRING AS origin_country,
                TO_TIMESTAMP_NTZ(raw_data[3]::NUMBER) AS time_position,
                raw_data[5]::FLOAT AS longitude,
                raw_data[6]::FLOAT AS latitude,
                ROUND(raw_data[7]::FLOAT * 3.28084, 1) AS altitude_feet,
                ROUND(raw_data[9]::FLOAT * 1.94384, 1) AS velocity_knots,
                raw_data[8]::BOOLEAN AS on_ground,
                CURRENT_TIMESTAMP() AS updated_at
            FROM BRONZE.RAW_FLIGHTS
            QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY time_position DESC) = 1;
        """)
        print("Silver transformation done.")
    finally:
        cur.close()
        conn.close()

def transform_gold(**kwargs):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE OR REPLACE TABLE GOLD.AIRSPACE_TRAFFIC_METRICS AS
            SELECT
                origin_country,
                COUNT(icao24) AS total_flights,
                COUNT_IF(on_ground = FALSE) AS airborne_flights,
                COUNT_IF(on_ground = TRUE) AS grounded_flights,
                ROUND(AVG(velocity_knots), 1) AS avg_speed_knots,
                ROUND(AVG(altitude_feet), 1) AS avg_altitude_feet
            FROM SILVER.CLEAN_FLIGHTS
            GROUP BY origin_country
            ORDER BY total_flights DESC;
        """)
        print("Gold layer updated.")
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id="flight_tracker_etl",
    start_date=datetime.datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["flights", "snowflake", "etl"]
) as dag:

    t1 = PythonOperator(task_id="extract_flights", python_callable=extract_flights)
    t2 = PythonOperator(task_id="load_bronze", python_callable=load_bronze)
    t3 = PythonOperator(task_id="transform_silver", python_callable=transform_silver)
    t4 = PythonOperator(task_id="transform_gold", python_callable=transform_gold)

    t1 >> t2 >> t3 >> t4
