from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = "216cdd8d10f0aae8f9d448893794f5e3"  # replace with your key

CITIES = [
    "London", "New York", "Tokyo", "Paris", "Dubai",
    "Mumbai", "Sydney", "Toronto", "Berlin", "Singapore"
]

DB_URL = "mysql+pymysql://root:root123@host.docker.internal:3306/weather_pipeline"

# ── Task 1: Extract ───────────────────────────────────────────────────────────
def extract(**context):
    print("📥 Extracting weather data from API...")
    raw_data = []
    for city in CITIES:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        res = requests.get(url)
        if res.status_code == 200:
            raw_data.append(res.json())
            print(f"✅ Extracted: {city}")
        else:
            print(f"❌ Failed: {city}")

    # Push to XCom so next task can use it
    context['ti'].xcom_push(key='raw_data', value=raw_data)
    print(f"✅ Total extracted: {len(raw_data)} cities")

# ── Task 2: Transform ─────────────────────────────────────────────────────────
def transform(**context):
    print("⚙️ Transforming data...")
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract')

    records = []
    for d in raw_data:
        records.append({
            "city":          d["name"],
            "country":       d["sys"]["country"],
            "latitude":      d["coord"]["lat"],
            "longitude":     d["coord"]["lon"],
            "temp_celsius":  round(d["main"]["temp"], 2),
            "feels_like":    round(d["main"]["feels_like"], 2),
            "temp_min":      round(d["main"]["temp_min"], 2),
            "temp_max":      round(d["main"]["temp_max"], 2),
            "humidity":      d["main"]["humidity"],
            "pressure":      d["main"]["pressure"],
            "wind_speed":    d["wind"]["speed"],
            "weather_main":  d["weather"][0]["main"],
            "weather_desc":  d["weather"][0]["description"],
            "cloudiness":    d["clouds"]["all"],
            "visibility":    d.get("visibility", 0),
            "recorded_at":   datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "extracted_at":  datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        })

    df = pd.DataFrame(records)

    # Data quality checks
    df = df.dropna(subset=["city", "temp_celsius"])
    df = df[df["temp_celsius"].between(-60, 60)]
    df = df[df["humidity"].between(0, 100)]

    # Push clean data
    context['ti'].xcom_push(key='clean_data', value=df.to_dict('records'))
    print(f"✅ Transformed {len(df)} records")

# ── Task 3: Load ──────────────────────────────────────────────────────────────
def load(**context):
    print(" Loading data to MySQL...")
    records = context['ti'].xcom_pull(key='clean_data', task_ids='transform')
    df = pd.DataFrame(records)

    engine = create_engine(DB_URL)
    df.to_sql(
        name="weather_data_airflow",
        con=engine,
        if_exists="append",
        index=False
    )
    print(f"✅ Loaded {len(df)} records to MySQL table 'weather_data_airflow'")

# ── DAG Definition ────────────────────────────────────────────────────────────
default_args = {
    'owner': 'dhairya',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='Real-time weather ETL pipeline — API to MySQL via Airflow',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etl', 'weather', 'mysql']
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )

    # Pipeline order
    extract_task >> transform_task >> load_task