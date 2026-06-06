# ⚡ Airflow Weather ETL Pipeline

Scheduled ETL pipeline orchestrated with Apache Airflow — 
automatically ingests real-time weather data every hour across 10 global cities.

## Pipeline Architecture
OpenWeatherMap API → Extract → Transform → Load → MySQL

## Screenshots

### Grid View — 3 Successful Runs
![Grid View](screenshots/Airflowgridview.png)

### Graph View — Extract → Transform → Load
![Graph View](screenshots/Airflowgraphview.png)

### Gantt View — Task Timing
![Gantt View](screenshots/Airflowghanttview.png)

### Code View — DAG Code
![Code View](screenshots/Airflowcodeview.png)

## Tech Stack
- Apache Airflow 2.8.1
- Docker & Docker Compose
- Python, Pandas
- MySQL
- OpenWeatherMap API
- XCom for inter-task data passing

## Results
- 3 successful automated runs
- Pipeline completes in 6 seconds
- Scheduled @hourly — runs automatically

## How to Run
1. Clone the repo
2. Add your API key in `dags/weather_etl_dag.py`
3. Run: `docker-compose up -d`
4. Open: http://localhost:8080
5. Login: admin / admin
6. Enable `weather_etl_pipeline` DAG