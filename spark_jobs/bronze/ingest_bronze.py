import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def ingest_bronze():
    
    spark = SparkSession.builder \
        .appName("Ingest Bronze") \
        .getOrCreate()
    
    # Paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    data_path = os.path.join(project_root, "data/")
    bronze_path = "data/bronze/"
    
    # Ingest Products
    products_df = spark.read.csv(f"{data_path}olist_products_dataset.csv", header=True, inferSchema=True)
    products_df.withColumn("Ingestion_timestamp", current_timestamp()) \
        .write.mode("overwrite").parquet(f'{bronze_path}products')
        
    # Ingest Customers
    customers_df = spark.read.csv(f"{data_path}olist_customers_dataset.csv", header=True, inferSchema=True)
    customers_df.withColumn("Ingestion_timestamp", current_timestamp()) \
        .write.mode("overwrite").parquet(f'{bronze_path}customers')
        
    # Ingest Geolocation
    geolocation_df = spark.read.csv(f"{data_path}olist_geolocation_dataset.csv", header=True, inferSchema=True)
    geolocation_df.withColumn("Ingestion_timestamp", current_timestamp()) \
        .write.mode("overwrite").parquet(f'{bronze_path}geolocation')
        
    # Ingest Orders
    orders_df = spark.read.csv(f"{data_path}olist_orders_dataset.csv", header=True, inferSchema=True)
    orders_df.withColumn("Ingestion_timestamp", current_timestamp()) \
        .write.mode("overwrite").parquet(f'{bronze_path}orders')
    
    # Ingest Orders
    order_items_df = spark.read.csv(f"{data_path}olist_order_items_dataset.csv", header=True, inferSchema=True)
    order_items_df.withColumn("Ingestion_timestamp", current_timestamp()) \
        .write.mode("overwrite").parquet(f'{bronze_path}order_items')
        
    
    spark.stop()
    
if __name__ == "__main__":
    ingest_bronze()