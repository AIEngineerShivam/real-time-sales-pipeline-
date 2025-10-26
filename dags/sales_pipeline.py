from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='Fetch and store sales data in PostgreSQL',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['sales', 'demo', 'etl'],
)

def get_db_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host='sales_postgres',
        port=5432,
        database='sales_data',
        user='salesuser',
        password='salespass123'
    )

def fetch_and_store_products(**context):
    """Fetch products from API and store in PostgreSQL"""
    try:
        # Fetch data from API
        response = requests.get('https://dummyjson.com/products?limit=30')
        response.raise_for_status()
        products = response.json().get('products', [])
        
        logging.info(f"✅ Fetched {len(products)} products from API")
        
        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert data
        inserted_count = 0
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
                    product.get('title'),
                    product.get('description'),
                    product.get('price'),
                    product.get('discountPercentage', 0),
                    product.get('rating', 0),
                    product.get('stock', 0),
                    product.get('brand', 'Unknown'),
                    product.get('category', 'Uncategorized'),
                    product.get('thumbnail', '')
                ))
                inserted_count += 1
            except Exception as e:
                logging.error(f"Error inserting product {product.get('id')}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"✅ Inserted/Updated {inserted_count} products in PostgreSQL")
        
        # Log first 3 products
        for product in products[:3]:
            logging.info(f"   📦 {product.get('title')} - ${product.get('price')} (Stock: {product.get('stock')})")
        
        return inserted_count
        
    except Exception as e:
        logging.error(f"❌ Error in fetch_and_store_products: {e}")
        raise

def calculate_summary(**context):
    """Calculate summary statistics"""
    try:
        conn = get_db_connection()
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
                ROUND(AVG(price)::numeric, 2) as avg_price,
                ROUND(AVG(rating)::numeric, 2) as avg_rating,
                SUM(stock) as total_stock
            FROM sales_data
            WHERE category IS NOT NULL AND brand IS NOT NULL
            GROUP BY category, brand
            ORDER BY total_products DESC
        """)
        
        conn.commit()
        
        # Get summary stats
        cursor.execute("SELECT COUNT(*) FROM sales_summary")
        summary_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logging.info(f"✅ Created {summary_count} summary records")
        return summary_count
        
    except Exception as e:
        logging.error(f"❌ Error in calculate_summary: {e}")
        raise

def generate_report(**context):
    """Generate final report"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total products
        cursor.execute("SELECT COUNT(*) FROM sales_data")
        total_products = cursor.fetchone()[0]
        
        # Get avg price
        cursor.execute("SELECT ROUND(AVG(price)::numeric, 2) FROM sales_data")
        avg_price = cursor.fetchone()[0]
        
        # Get total stock
        cursor.execute("SELECT SUM(stock) FROM sales_data")
        total_stock = cursor.fetchone()[0]
        
        # Get top 5 products by price
        cursor.execute("""
            SELECT title, price, brand 
            FROM sales_data 
            ORDER BY price DESC 
            LIMIT 5
        """)
        top_products = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logging.info("=" * 60)
        logging.info("📈 SALES PIPELINE REPORT")
        logging.info("=" * 60)
        logging.info(f"⏰ Execution Time: {datetime.now()}")
        logging.info(f"📦 Total Products: {total_products}")
        logging.info(f"💰 Average Price: ${avg_price}")
        logging.info(f"📊 Total Stock: {total_stock}")
        logging.info("")
        logging.info("🏆 Top 5 Most Expensive Products:")
        for idx, (title, price, brand) in enumerate(top_products, 1):
            logging.info(f"   {idx}. {title} (${price}) - {brand}")
        logging.info("=" * 60)
        
        return "Report generated successfully"
        
    except Exception as e:
        logging.error(f"❌ Error in generate_report: {e}")
        raise

# Define tasks
task_fetch = PythonOperator(
    task_id='fetch_and_store_products',
    python_callable=fetch_and_store_products,
    dag=dag,
)

task_summary = PythonOperator(
    task_id='calculate_summary',
    python_callable=calculate_summary,
    dag=dag,
)

task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Set dependencies
task_fetch >> task_summary >> task_report
