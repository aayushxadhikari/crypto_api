CRYPTO_API – Crypto Market ETL Pipeline (Airflow + Prefect + Docker)

This project implements an automated ETL pipeline for extracting cryptocurrency market data from external APIs, storing raw data, transforming it, and loading it into a warehouse for analytics.
It supports two orchestration engines: Apache Airflow (production) and Prefect (lightweight).

The entire pipeline can be run locally using Docker Compose.

📁 Project Structure
CRYPTO_API/
│
├── airflow/
│   └── dags/
│       └── crypto_api_warehouse_dag1.py      # Airflow DAG for ETL
│
├── docker-compose.yml                         # Airflow + Postgres Docker setup
│
├── landing_raw/                               # Raw JSON crypto API data
│
├── src/
│   └── prefect_flow.py                        # Prefect implementation of ETL
│
├── venv/                                      # Local Python virtual environment
│
├── requirements.txt                           # Python dependencies
└── README.md

🚀 Overview

This application collects real-time cryptocurrency price data and loads it into a warehouse. It includes:

✔ Extraction

Fetch crypto prices via REST API

Save raw API responses to landing_raw/

✔ Transformation

Flatten JSON

Select relevant fields (symbol, price, timestamp, etc.)

✔ Load

Load transformed data into a data warehouse (PostgreSQL or any DB)

✔ Orchestration

Airflow DAG (Production / Scheduler)

Prefect Flow (Lightweight local runs)

🐳 Running With Docker (Recommended)
1️⃣ Start Airflow + Services

From the project root:

docker-compose up --build


This boots:

Airflow Scheduler

Airflow Webserver

Postgres (if configured)

2️⃣ Access Airflow UI

👉 http://localhost:8080

Default login:

user: airflow
pass: airflow

3️⃣ Run the ETL

Enable and trigger:

crypto_api_warehouse_dag1

▶️ Running the Prefect Flow

If you want to run ETL without Airflow:

python src/prefect_flow.py


This fetches crypto data and writes it to landing_raw/.

📦 Data Flow
1. Raw Zone → /landing_raw/

All API responses are stored as timestamped .json files:

landing_raw/
   ├── btc_2025-11-16.json
   ├── eth_2025-11-16.json
   └── ...

2. Warehouse (PostgreSQL or Any DB)

The DAG/Flow loads cleaned data into your analytical database.

A typical schema:

column	type
symbol	TEXT
price_usd	FLOAT
time_fetched	TIMESTAMP
⚙️ Installation (Local Python Option)
python3 -m venv venv
source venv/bin/activate         # macOS/Linux
venv\Scripts\activate            # Windows

pip install -r requirements.txt


🌱 Future Improvements

Add DBT transformations

Persist Parquet files

Add S3/GCS storage layers

Add metrics monitoring (Grafana/Prometheus)

Add retry/alerting in Airflow and Prefect