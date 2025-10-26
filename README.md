# 🚀 Real-Time Sales Data Pipeline

A complete end-to-end data engineering project using Kafka, Spark, Airflow, and PostgreSQL.

## 🏗️ Architecture

- **Data Source**: DummyJSON API (Mock E-commerce Data)
- **Message Queue**: Apache Kafka
- **Stream Processing**: Apache Spark
- **Orchestration**: Apache Airflow
- **Database**: PostgreSQL
- **Visualization**: Power BI (Export Ready)

## 📋 Prerequisites

- Docker Desktop for Mac (M2 Optimized)
- VS Code
- 8GB RAM minimum
- 10GB free disk space

## 🚀 Quick Start

### 1. Start All Services
```bash
docker-compose up -d
```

### 2. Check Service Status
```bash
docker-compose ps
```

### 3. Access Web Interfaces

- **Airflow**: http://localhost:8081 (username: `admin`, password: `admin`)
- **Spark Master**: http://localhost:8080
- **Kafka**: localhost:9093

### 4. View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka-producer
docker-compose logs -f airflow-webserver
```

## 📊 Airflow Setup

1. Access Airflow at http://localhost:8081
2. Login with `admin` / `admin`
3. Enable the DAG: `sales_etl_pipeline`
4. Trigger the DAG manually or wait for scheduled run

### Configure PostgreSQL Connection in Airflow

1. Go to Admin → Connections
2. Add new connection:
   - **Conn Id**: `postgres_default`
   - **Conn Type**: `Postgres`
   - **Host**: `postgres`
   - **Schema**: `sales_data`
   - **Login**: `salesuser`
   - **Password**: `salespass123`
   - **Port**: `5432`

## 🔍 Verify Data Flow

### Check Kafka Topics
```bash
docker exec -it sales_kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Check PostgreSQL Data
```bash
docker exec -it sales_postgres psql -U salesuser -d sales_data -c "SELECT COUNT(*) FROM sales_data;"
```

### View Real-time Data
```bash
docker exec -it sales_postgres psql -U salesuser -d sales_data -c "SELECT * FROM sales_data LIMIT 10;"
```

## 🛠️ Useful Commands

### Restart Services
```bash
docker-compose restart
```

### Stop Services
```bash
docker-compose down
```

### Remove All Data and Start Fresh
```bash
docker-compose down -v
docker-compose up -d
```

### Execute SQL Queries
```bash
docker exec -it sales_postgres psql -U salesuser -d sales_data
```

## 📈 Power BI Integration

1. Export data:
```bash
docker exec -it sales_airflow_webserver python /opt/airflow/scripts/powerbi_connector.py
```

2. Files will be available in `./data/processed/`
3. Import CSV files into Power BI Desktop

## 🐛 Troubleshooting

### Airflow not starting
```bash
docker-compose logs airflow-webserver
docker-compose restart airflow-webserver
```

### Kafka connection issues
```bash
docker-compose restart zookeeper kafka
```

### PostgreSQL connection refused
```bash
docker-compose restart postgres
```

## 📝 Project Structure
```
real_time_sales_pipeline/
├── airflow/          # Airflow DAGs and configs
├── kafka/            # Kafka producers and consumers
├── spark/            # Spark streaming jobs
├── postgres/         # Database initialization
├── scripts/          # Helper scripts
└── data/             # Data storage
```

## 🎯 Features

- ✅ Real-time data ingestion from API
- ✅ Stream processing with Spark
- ✅ Automated ETL with Airflow
- ✅ Data storage in PostgreSQL
- ✅ Export ready for Power BI
- ✅ M2 Mac optimized
- ✅ Docker containerized

## 📧 Support

For issues or questions, check the logs:
```bash
docker-compose logs -f [service-name]
```