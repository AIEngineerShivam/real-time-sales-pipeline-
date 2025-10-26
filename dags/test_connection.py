from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'test_connection',
    default_args=default_args,
    description='Test database connection',
    schedule_interval=None,
    catchup=False,
    tags=['test'],
)

def test_imports():
    """Test if packages are available"""
    try:
        import psycopg2
        logging.info("✅ psycopg2 imported successfully")
        
        import requests
        logging.info("✅ requests imported successfully")
        
        return "All imports successful"
    except Exception as e:
        logging.error(f"❌ Import error: {e}")
        raise

def test_database():
    """Test database connection"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='sales_postgres',
            port=5432,
            database='sales_data',
            user='salesuser',
            password='salespass123'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logging.info(f"✅ Database connected: {version[0]}")
        cursor.close()
        conn.close()
        return "Database connection successful"
    except Exception as e:
        logging.error(f"❌ Database error: {e}")
        raise

def test_api():
    """Test API connection"""
    try:
        import requests
        response = requests.get('https://dummyjson.com/products?limit=1')
        response.raise_for_status()
        data = response.json()
        logging.info(f"✅ API connected: Got product {data['products'][0]['title']}")
        return "API connection successful"
    except Exception as e:
        logging.error(f"❌ API error: {e}")
        raise

task1 = PythonOperator(
    task_id='test_imports',
    python_callable=test_imports,
    dag=dag,
)

task2 = PythonOperator(
    task_id='test_database',
    python_callable=test_database,
    dag=dag,
)

task3 = PythonOperator(
    task_id='test_api',
    python_callable=test_api,
    dag=dag,
)

task1 >> task2 >> task3
