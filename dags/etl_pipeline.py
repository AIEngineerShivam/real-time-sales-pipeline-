from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['sales', 'etl'],
)

def fetch_and_store_data(**context):
    """Fetch data from API and store in PostgreSQL"""
    try:
        # Fetch data
        response = requests.get('https://dummyjson.com/products')
        response.raise_for_status()
        products = response.json().get('products', [])
        
        # Get PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Insert data
        inserted_count = 0
        for product in products:
            cursor.execute("""
                INSERT INTO sales_data 
                (product_id, title, description, price, discount_percentage, 
                 rating, stock, brand, category, thumbnail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    stock = EXCLUDED.stock,
                    processed_at = CURRENT_TIMESTAMP
            """, (
                product.get('id'),
                product.get('title'),
                product.get('description'),
                product.get('price'),
                product.get('discountPercentage'),
                product.get('rating'),
                product.get('stock'),
                product.get('brand'),
                product.get('category'),
                product.get('thumbnail')
            ))
            inserted_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"✅ Inserted/Updated {inserted_count} products")
        return inserted_count
        
    except Exception as e:
        logging.error(f"❌ Error in fetch_and_store_data: {e}")
        raise

def aggregate_sales_data(**context):
    """Create aggregated sales summary"""
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Clear old summary
        cursor.execute("TRUNCATE TABLE sales_summary")
        
        # Insert aggregated data
        cursor.execute("""
            INSERT INTO sales_summary (category, brand, total_products, avg_price, avg_rating, total_stock)
            SELECT 
                category,
                brand,
                COUNT(*) as total_products,
                AVG(price) as avg_price,
                AVG(rating) as avg_rating,
                SUM(stock) as total_stock
            FROM sales_data
            WHERE category IS NOT NULL AND brand IS NOT NULL
            GROUP BY category, brand
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("✅ Sales summary aggregated successfully")
        
    except Exception as e:
        logging.error(f"❌ Error in aggregate_sales_data: {e}")
        raise

def calculate_metrics(**context):
    """Calculate real-time metrics"""
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        metrics = [
            ("total_products", "SELECT COUNT(*) FROM sales_data"),
            ("avg_price", "SELECT AVG(price) FROM sales_data"),
            ("avg_rating", "SELECT AVG(rating) FROM sales_data"),
            ("total_stock", "SELECT SUM(stock) FROM sales_data"),
            ("unique_categories", "SELECT COUNT(DISTINCT category) FROM sales_data"),
            ("unique_brands", "SELECT COUNT(DISTINCT brand) FROM sales_data"),
        ]
        
        for metric_name, query in metrics:
            cursor.execute(query)
            value = cursor.fetchone()[0]
            
            cursor.execute("""
                INSERT INTO realtime_metrics (metric_name, metric_value)
                VALUES (%s, %s)
            """, (metric_name, value or 0))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("✅ Metrics calculated successfully")
        
    except Exception as e:
        logging.error(f"❌ Error in calculate_metrics: {e}")
        raise

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_and_store_data',
    python_callable=fetch_and_store_data,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_sales_data',
    python_callable=aggregate_sales_data,
    dag=dag,
)

metrics_task = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics,
    dag=dag,
)

# Set task dependencies
fetch_data_task >> aggregate_task >> metrics_task