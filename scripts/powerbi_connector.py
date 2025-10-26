import psycopg2
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PowerBIConnector:
    def __init__(self):
        self.conn_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'sales_data',
            'user': 'salesuser',
            'password': 'salespass123'
        }
    
    def get_connection(self):
        """Create database connection"""
        try:
            conn = psycopg2.connect(**self.conn_params)
            logger.info("✅ Connected to PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"❌ Connection error: {e}")
            return None
    
    def export_sales_data(self, output_file="/data/processed/sales_export.csv"):
        """Export sales data for Power BI"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
            
            query = """
                SELECT 
                    product_id,
                    title,
                    price,
                    discount_percentage,
                    price * (1 - discount_percentage/100) as final_price,
                    rating,
                    stock,
                    brand,
                    category,
                    created_at,
                    processed_at
                FROM sales_data
                ORDER BY created_at DESC
            """
            
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_file, index=False)
            
            conn.close()
            logger.info(f"✅ Exported {len(df)} records to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Export error: {e}")
            return False
    
    def export_summary(self, output_file="/data/processed/sales_summary.csv"):
        """Export sales summary for Power BI"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
            
            query = """
                SELECT 
                    category,
                    brand,
                    total_products,
                    avg_price,
                    avg_rating,
                    total_stock,
                    last_updated
                FROM sales_summary
                ORDER BY total_products DESC
            """
            
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_file, index=False)
            
            conn.close()
            logger.info(f"✅ Exported summary to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Export error: {e}")
            return False

if __name__ == "__main__":
    connector = PowerBIConnector()
    connector.export_sales_data()
    connector.export_summary()