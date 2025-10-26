from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'working_sales_dag',
    default_args=default_args,
    description='Working sales data pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['sales', 'working'],
)

def insert_data():
    """Fetch and insert data"""
    try:
        # Fetch data
        logging.info("🔄 Fetching data from API...")
        response = requests.get('https://dummyjson.com/products?limit=30', timeout=10)
        response.raise_for_status()
        products = response.json().get('products', [])
        logging.info(f"✅ Fetched {len(products)} products")
        
        # Connect to database
        logging.info("🔄 Connecting to database...")
        conn = psycopg2.connect(
            host='sales_postgres',
            port=5432,
            database='sales_data',
            user='salesuser',
            password='salespass123'
        )
        cursor = conn.cursor()
        logging.info("✅ Connected to database")
        
        # Insert products
        inserted = 0
        for product in products:
            try:
                cursor.execute("""
                    INSERT INTO sales_data 
                    (product_id, title, description, price, discount_percentage, 
                     rating, stock, brand, category, thumbnail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        price = EXCLUDED.price,
                        stock = EXCLUDED.stock,
                        rating = EXCLUDED.rating,
                        processed_at = CURRENT_TIMESTAMP
                """, (
                    product.get('id'),
                    product.get('title', 'Unknown'),
                    product.get('description', ''),
                    float(product.get('price', 0)),
                    float(product.get('discountPercentage', 0)),
                    float(product.get('rating', 0)),
                    int(product.get('stock', 0)),
                    product.get('brand', 'Unknown'),
                    product.get('category', 'Uncategorized'),
                    product.get('thumbnail', '')
                ))
                inserted += 1
                
                if inserted <= 3:
                    logging.info(f"   📦 Inserted: {product.get('title')} - ${product.get('price')}")
                    
            except Exception as e:
                logging.error(f"❌ Error inserting product {product.get('id')}: {e}")
                continue
        
        # CRITICAL: Commit the transaction!
        conn.commit()
        logging.info(f"✅ Committed {inserted} products to database")
        
        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM sales_data")
        count = cursor.fetchone()[0]
        logging.info(f"✅ Total records in database: {count}")
        
        cursor.close()
        conn.close()
        
        return f"Successfully inserted {inserted} products"
        
    except Exception as e:
        logging.error(f"❌ Error: {e}")
        raise

task = PythonOperator(
    task_id='insert_sales_data',
    python_callable=insert_data,
    dag=dag,
)
