from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hourly_sales_dag',
    default_args=default_args,
    description='Automatically fetch sales data every hour',
    schedule_interval=timedelta(hours=1),  # ⏰ RUNS EVERY HOUR
    catchup=False,
    tags=['sales', 'hourly', 'production'],
)

def fetch_and_update_sales():
    """Fetch products from API and update database"""
    try:
        logging.info("=" * 60)
        logging.info("🔄 HOURLY SALES DATA UPDATE STARTED")
        logging.info("=" * 60)
        logging.info(f"⏰ Execution Time: {datetime.now()}")
        
        # Fetch from API
        logging.info("📡 Fetching products from API...")
        response = requests.get('https://dummyjson.com/products?limit=30', timeout=10)
        response.raise_for_status()
        products = response.json().get('products', [])
        logging.info(f"✅ Successfully fetched {len(products)} products")
        
        # Connect to database
        logging.info("🔌 Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host='sales_postgres',
            port=5432,
            database='sales_data',
            user='salesuser',
            password='salespass123'
        )
        cursor = conn.cursor()
        logging.info("✅ Database connection established")
        
        # Insert/Update products
        logging.info("💾 Updating database...")
        updated = 0
        new = 0
        
        for product in products:
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
                RETURNING (xmax = 0) AS inserted
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
            
            was_inserted = cursor.fetchone()[0]
            if was_inserted:
                new += 1
            else:
                updated += 1
        
        # Commit transaction
        conn.commit()
        logging.info(f"✅ Database updated successfully!")
        logging.info(f"   📦 New products: {new}")
        logging.info(f"   🔄 Updated products: {updated}")
        
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                ROUND(AVG(price)::numeric, 2) as avg_price,
                ROUND(AVG(rating)::numeric, 2) as avg_rating,
                SUM(stock) as total_stock
            FROM sales_data
        """)
        stats = cursor.fetchone()
        
        logging.info("📊 Database Statistics:")
        logging.info(f"   Total Products: {stats[0]}")
        logging.info(f"   Average Price: ${stats[1]}")
        logging.info(f"   Average Rating: {stats[2]}/5")
        logging.info(f"   Total Stock: {stats[3]} units")
        
        cursor.close()
        conn.close()
        
        logging.info("=" * 60)
        logging.info("✅ HOURLY UPDATE COMPLETED SUCCESSFULLY")
        logging.info("=" * 60)
        
        return {
            'status': 'success',
            'new': new,
            'updated': updated,
            'total': stats[0]
        }
        
    except Exception as e:
        logging.error("=" * 60)
        logging.error(f"❌ ERROR DURING HOURLY UPDATE: {e}")
        logging.error("=" * 60)
        raise

task = PythonOperator(
    task_id='hourly_sales_update',
    python_callable=fetch_and_update_sales,
    dag=dag,
)
