# real-time-sales-pipeline-
A production-ready ETL pipeline for real-time sales data analytics. Built with Apache Airflow for orchestration, PostgreSQL for storage, Docker for containerization.Features automated hourly data fetching, transformation, and storage with web-based monitoring via Adminer. Includes complete setup, DAG scheduling, and data visualization capabilities.
# Real-Time Sales Data Pipeline

A simple ETL pipeline that automatically fetches sales data from an API, processes it, and stores it in PostgreSQL using Apache Airflow.

## Features

- Automated hourly data updates
- Apache Airflow for scheduling
- PostgreSQL database
- Web UI for monitoring and data viewing
- Fully containerized with Docker

## Tech Stack

- Apache Airflow 2.7.3
- PostgreSQL 15
- Python 3.11
- Docker & Docker Compose
- Adminer (Database UI)

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- 8GB RAM

### Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/real-time-sales-pipeline.git
cd real-time-sales-pipeline
```

2. Start the services:
```bash
docker-compose up -d
```

3. Wait 60 seconds for services to start

4. Access the interfaces:
   - **Airflow UI:** http://localhost:8081 (admin/admin)
   - **Database UI (Adminer):** http://localhost:8082 (salesuser/salespass123)

## Usage

### Trigger pipeline manually:
```bash
docker exec sales_airflow airflow dags trigger hourly_sales_dag
```

### View data:
```bash
docker exec sales_postgres psql -U salesuser -d sales_data -c "SELECT * FROM sales_data LIMIT 10;"
```

### Stop services:
```bash
docker-compose down
```

## Project Structure
```
real-time-sales-pipeline/
├── airflow/dags/          # Pipeline workflows
├── postgres/init.sql      # Database setup
├── docker-compose.yml     # Container configuration
└── README.md
```

## Database Schema

**sales_data table:**
- product_id, title, description
- price, discount_percentage
- rating, stock
- brand, category
- processed_at (timestamp)

## License

MIT License



