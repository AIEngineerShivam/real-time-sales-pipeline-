from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SalesStreamProcessor") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/data/checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("discountPercentage", DoubleType(), True),
    StructField("rating", DoubleType(), True),
    StructField("stock", IntegerType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

def process_stream():
    """Process Kafka stream and write to PostgreSQL"""
    try:
        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "sales_events") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Add processing timestamp
        processed_df = parsed_df.withColumn("processed_at", current_timestamp())
        
        # Write to PostgreSQL
        query = processed_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .start()
        
        logger.info("✅ Spark Streaming job started")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"❌ Error in stream processing: {e}")
        raise

def write_to_postgres(batch_df, batch_id):
    """Write batch to PostgreSQL"""
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/sales_data") \
            .option("dbtable", "sales_data") \
            .option("user", "salesuser") \
            .option("password", "salespass123") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"✅ Batch {batch_id} written to PostgreSQL")
    except Exception as e:
        logger.error(f"❌ Error writing batch {batch_id}: {e}")

if __name__ == "__main__":
    process_stream()