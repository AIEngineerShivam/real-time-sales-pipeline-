import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self):
        pass
    
    def clean_products(self, products):
        """Clean and validate product data"""
        try:
            df = pd.DataFrame(products)
            
            # Remove duplicates
            df = df.drop_duplicates(subset=['id'])
            
            # Handle missing values
            df['brand'] = df['brand'].fillna('Unknown')
            df['category'] = df['category'].fillna('Uncategorized')
            df['description'] = df['description'].fillna('')
            
            # Validate numeric fields
            df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
            df['stock'] = pd.to_numeric(df['stock'], errors='coerce').fillna(0)
            df['discountPercentage'] = pd.to_numeric(df['discountPercentage'], errors='coerce').fillna(0)
            
            # Remove invalid records
            df = df[df['price'] > 0]
            df = df[df['rating'] >= 0]
            df = df[df['stock'] >= 0]
            
            logger.info(f"✅ Cleaned {len(df)} products")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"❌ Error cleaning data: {e}")
            return []
    
    def enrich_data(self, products):
        """Enrich product data with additional fields"""
        try:
            df = pd.DataFrame(products)
            
            # Add calculated fields
            df['final_price'] = df['price'] * (1 - df['discountPercentage'] / 100)
            df['price_category'] = pd.cut(df['price'], 
                                          bins=[0, 50, 200, float('inf')],
                                          labels=['Budget', 'Mid-Range', 'Premium'])
            df['rating_category'] = pd.cut(df['rating'],
                                           bins=[0, 3, 4, 4.5, 5],
                                           labels=['Poor', 'Average', 'Good', 'Excellent'])
            
            logger.info(f"✅ Enriched {len(df)} products")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"❌ Error enriching data: {e}")
            return products

if __name__ == "__main__":
    cleaner = DataCleaner()