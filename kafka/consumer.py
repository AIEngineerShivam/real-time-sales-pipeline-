import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales_events')

def create_consumer():
    """Create Kafka consumer"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='sales_consumer_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"✅ Consumer connected to Kafka")
            return consumer
        except KafkaError as e:
            retry_count += 1
            logger.warning(f"⚠️ Connection attempt {retry_count}/{max_retries} failed")
            time.sleep(5)
    
    raise Exception("Failed to connect to Kafka")

def consume_messages():
    """Consume messages from Kafka"""
    consumer = create_consumer()
    
    try:
        logger.info(f"👂 Listening for messages on topic: {KAFKA_TOPIC}")
        
        for message in consumer:
            product = message.value
            logger.info(f"📥 Received: {product.get('title')} (ID: {product.get('id')})")
            
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()