# Crypto Market ETL Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?logo=docker&logoColor=white)](https://www.docker.com/)

A production-ready ETL pipeline for extracting, transforming, and loading cryptocurrency market data. Built with **Apache Airflow** and **Prefect** orchestration engines, containerized with Docker for seamless deployment.

## 🎯 Overview

This project implements an automated ETL (Extract, Transform, Load) pipeline that:
- Fetches real-time cryptocurrency market data from external APIs
- Transforms and cleans data for analytics
- Loads processed data into a PostgreSQL data warehouse
- Supports dual orchestration: Airflow (production) and Prefect (development)

## ✨ Features

- **Multi-Orchestrator Support**: Choose between Apache Airflow or Prefect based on your needs
- **Dockerized Deployment**: Complete containerization for consistent environments
- **Raw Data Persistence**: All API responses saved as timestamped JSON files
- **Scalable Architecture**: Modular design for easy extension to multiple crypto sources
- **Data Quality**: Built-in validation and transformation steps
- **Retry Logic**: Automatic retry mechanisms for failed API calls
- **Incremental Loading**: Efficient data warehouse updates

## 🏗️ Architecture

```
┌─────────────────┐
│  Crypto APIs    │
│ (CoinGecko, etc)│
└────────┬────────┘
         │
         │ Extract
         ▼
┌─────────────────┐
│  Landing Zone   │
│  (Raw JSON)     │
└────────┬────────┘
         │
         │ Transform
         ▼
┌─────────────────┐
│  Staging Area   │
│  (Cleaned Data) │
└────────┬────────┘
         │
         │ Load
         ▼
┌─────────────────┐
│   PostgreSQL    │
│  Data Warehouse │
└─────────────────┘
```

**Tech Stack:**
- **Orchestration**: Apache Airflow 2.x, Prefect 2.x
- **Storage**: PostgreSQL 13+
- **Containerization**: Docker, Docker Compose
- **Language**: Python 3.8+
- **Data Processing**: Pandas, SQLAlchemy

## 📦 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker** (20.10+) and **Docker Compose** (2.0+)
- **Python** (3.8+) - for local development

## 🚀 Quick Start

Get the pipeline running in 3 simple steps:

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/crypto_api.git
cd crypto_api

# 2. Start all services with Docker Compose
docker-compose up --build -d

# 3. Access Airflow UI
# Open browser: http://localhost:8080
# Login: airflow / airflow
```

## 💻 Installation

### Option 1: Docker (Recommended)

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

### Option 2: Local Python Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize Airflow database (first time only)
airflow db init

# Create Airflow admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start Airflow webserver
airflow webserver --port 8080

# In a new terminal, start scheduler
airflow scheduler
```


### Running with Prefect

For lightweight local runs without Airflow:

```bash
# Activate your virtual environment
source venv/bin/activate

# Run the Prefect flow
python src/prefect_flow.py

# Monitor in Prefect UI (optional)
prefect server start
# Then navigate to http://localhost:4200
```

## 🔄 Data Flow

### 1. **Extract Phase**
- Fetches cryptocurrency data from external APIs (CoinGecko, Binance, etc.)
- Stores raw API responses in `landing_raw/` as timestamped JSON files
- Format: `{symbol}_{YYYY-MM-DD_HH-MM-SS}.json`

### 2. **Transform Phase**
- Reads raw JSON files
- Flattens nested structures
- Extracts relevant fields:
  - `symbol`: Cryptocurrency ticker (e.g., BTC, ETH)
  - `price_usd`: Current price in USD
  - `volume_24h`: 24-hour trading volume
  - `market_cap`: Total market capitalization
  - `timestamp`: Data fetch timestamp
- Performs data validation and cleaning

### 3. **Load Phase**
- Connects to PostgreSQL warehouse
- Creates tables if they don't exist
- Performs upsert operations (insert or update)
- Maintains data lineage

### Warehouse Schema

```sql
CREATE TABLE crypto_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price_usd DECIMAL(20, 8),
    volume_24h DECIMAL(20, 2),
    market_cap DECIMAL(20, 2),
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

CREATE INDEX idx_symbol_timestamp ON crypto_prices(symbol, timestamp);
```

