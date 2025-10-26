from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("SalesTransformations") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def read_from_postgres(spark, table_name):
    """Read data from PostgreSQL"""
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/sales_data") \
        .option("dbtable", table_name) \
        .option("user", "salesuser") \
        .option("password", "salespass123") \
        .option("driver", "org.postgresql.Driver") \
        .load()

def transform_sales_data():
    """Apply transformations on sales data"""
    spark = create_spark_session()
    
    try:
        # Read sales data
        df = read_from_postgres(spark, "sales_data")
        
        # Transformations
        transformed_df = df \
            .withColumn("final_price", col("price") * (1 - col("discount_percentage") / 100)) \
            .withColumn("price_category", 
                when(col("price") < 50, "Budget")
                .when((col("price") >= 50) & (col("price") < 200), "Mid-Range")
                .otherwise("Premium")) \
            .withColumn("rating_category",
                when(col("rating") >= 4.5, "Excellent")
                .when((col("rating") >= 4.0) & (col("rating") < 4.5), "Good")
                .when((col("rating") >= 3.0) & (col("rating") < 4.0), "Average")
                .otherwise("Poor"))
        
        # Aggregate by category
        category_stats = transformed_df.groupBy("category") \
            .agg(
                count("*").alias("total_products"),
                avg("price").alias("avg_price"),
                avg("rating").alias("avg_rating"),
                sum("stock").alias("total_stock"),
                avg("final_price").alias("avg_final_price")
            )
        
        logger.info("✅ Transformations completed")
        return transformed_df, category_stats
        
    except Exception as e:
        logger.error(f"❌ Error in transformations: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_sales_data()