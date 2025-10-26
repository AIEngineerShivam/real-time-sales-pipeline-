import requests
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIFetcher:
    def __init__(self, api_url="https://dummyjson.com/products"):
        self.api_url = api_url
    
    def fetch_products(self, limit=100):
        """Fetch products from API"""
        try:
            response = requests.get(f"{self.api_url}?limit={limit}", timeout=10)
            response.raise_for_status()
            data = response.json()
            
            products = data.get('products', [])
            logger.info(f"✅ Fetched {len(products)} products")
            return products
            
        except Exception as e:
            logger.error(f"❌ Error fetching products: {e}")
            return []
    
    def fetch_single_product(self, product_id):
        """Fetch single product by ID"""
        try:
            response = requests.get(f"{self.api_url}/{product_id}", timeout=10)
            response.raise_for_status()
            product = response.json()
            
            logger.info(f"✅ Fetched product: {product.get('title')}")
            return product
            
        except Exception as e:
            logger.error(f"❌ Error fetching product {product_id}: {e}")
            return None
    
    def save_to_file(self, products, filename="products.json"):
        """Save products to JSON file"""
        try:
            with open(f"/data/raw/{filename}", 'w') as f:
                json.dump(products, f, indent=2)
            logger.info(f"✅ Saved {len(products)} products to {filename}")
        except Exception as e:
            logger.error(f"❌ Error saving to file: {e}")

if __name__ == "__main__":
    fetcher = APIFetcher()
    products = fetcher.fetch_products()
    if products:
        fetcher.save_to_file(products)