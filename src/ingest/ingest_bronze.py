import yaml
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from schemas import (
    PRODUCTS_SCHEMA, CUSTOMERS_SCHEMA,
    GEOLOCATION_SCHEMA, ORDER_ITEMS_SCHEMA,
    ORDER_PAYMENTS_SCHEMA, ORDER_REVIEWS_SCHEMA,
    ORDERS_SCHEMA, SELLERS_SCHEMA
)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../conf/config.yaml")

with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)
    

def get_spark():
    return (
        SparkSession.builder
        .appName("Ingest Bronze")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def read_csv_with_schema(path, schema, spark):
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )

def write_bronze(df, base_path, table, run_date, run_id):
    enriched = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("run_date", lit(run_date))
        .withColumn("run_id", lit(run_id))
    )
    
    enriched.write.mode("overwrite").partitionBy("run_date").parquet(os.path.join(base_path, table))
    print(f"[INGEST] Wrote bronze/{table} run_date={run_date}")
    
def ingest_bronze():
    spark = get_spark()
    
    # Paths
    data_path = config['paths']['landing']
    bronze_path = config['paths']['bronze']
    
    # Metadata
    run_date = datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    run_id = datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
    
    # Products
    products = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['products']), PRODUCTS_SCHEMA
    )
    write_bronze(products, bronze_path, "products", run_date, run_id)
    
    # Customers
    customers = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['customers']), CUSTOMERS_SCHEMA
    )
    write_bronze(customers, bronze_path, "customers", run_date, run_id)
    
    # Orders
    orders = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['orders']), ORDERS_SCHEMA
    )
    write_bronze(orders, bronze_path, "orders", run_date, run_id)
    
    # Order Items
    order_items = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['order_items']), ORDER_ITEMS_SCHEMA
    )
    write_bronze(order_items, bronze_path, "order_items", run_date, run_id)

    # Order Payments
    order_payments = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['order_payments']), ORDER_PAYMENTS_SCHEMA
    )
    write_bronze(order_payments, bronze_path, "order_payments", run_date, run_id)
    
    # Order Reviews
    order_reviews = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['order_reviews']), ORDER_REVIEWS_SCHEMA
    )
    write_bronze(order_reviews, bronze_path, "order_reviews", run_date, run_id)
    
    # Geolocation
    geolocation = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['geolocation']), GEOLOCATION_SCHEMA
    )
    write_bronze(geolocation, bronze_path, "geolocation", run_date, run_id)

    # Sellers
    sellers = read_csv_with_schema(
        spark, os.path.join(data_path, config['datasets']['sellers']), SELLERS_SCHEMA
    )
    write_bronze(sellers, bronze_path, "sellers", run_date, run_id)
    
    spark.stop()
    
if __name__ == "__main__":
    ingest_bronze()
    