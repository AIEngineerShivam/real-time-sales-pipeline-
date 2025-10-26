import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales_events')
API_URL = os.getenv('API_URL', 'https://dummyjson.com/products')

def create_producer():
    """Create Kafka producer with retry logic"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"✅ Successfully connected to Kafka at {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            retry_count += 1
            logger.warning(f"⚠️ Kafka connection attempt {retry_count}/{max_retries} failed: {e}")
            time.sleep(5)
    
    raise Exception("Failed to connect to Kafka after maximum retries")

def fetch_products():
    """Fetch products from API"""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('products', [])
    except Exception as e:
        logger.error(f"❌ Error fetching products: {e}")
        return []

def produce_messages():
    """Main producer loop"""
    producer = create_producer()
    
    try:
        logger.info(f"🚀 Starting to produce messages to topic: {KAFKA_TOPIC}")
        
        while True:
            products = fetch_products()
            
            if products:
                for product in products:
                    # Add timestamp
                    product['timestamp'] = time.time()
                    
                    # Send to Kafka
                    future = producer.send(KAFKA_TOPIC, value=product)
                    
                    try:
                        record_metadata = future.get(timeout=10)
                        logger.info(f"📤 Sent product ID {product.get('id')} to partition {record_metadata.partition}")
                    except KafkaError as e:
                        logger.error(f"❌ Failed to send message: {e}")
                
                logger.info(f"✅ Sent {len(products)} products to Kafka")
            else:
                logger.warning("⚠️ No products fetched")
            
            # Wait before next batch
            time.sleep(30)
            
    except KeyboardInterrupt:
        logger.info("🛑 Producer stopped by user")
    except Exception as e:
        logger.error(f"❌ Producer error: {e}")
    finally:
        producer.close()
        logger.info("👋 Producer closed")

if __name__ == "__main__":
    produce_messages()