import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from transform.transform_tables import (
    transform_customers, transform_order_payments,
    transform_geolocation, transform_order_items,
    transform_order_reviews, transform_orders,
    transform_products, transform_sellers
)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../conf/config.yaml")

with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)
    
def get_spark(app_name="Ingest Silver"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def write_silver_table(df, silver_base_path, table, run_date=None):
    dest = os.path.join(silver_base_path, table)
    if run_date:
        df.withColumn("run_date", col("run_date")) \
        .write.mode("overwrite") \
        .partitionBy("run_date").parquet(dest)
    else:
        df.write.mode("overwrite").parquet(dest)
    print(f"[TRANSFORM] Wrote Silver/{table}")
    
def ingest_silver():
    
    
    spark = get_spark()
    
    
    bronze_path = config['paths']['bronze']
    silver_path = config['paths']['silver']
    
    customers = transform_customers(spark, bronze_path)
    
    write_silver_table(customers, silver_path, "customers")
    
    products = transform_products(spark, bronze_path)
    
    write_silver_table(products, silver_path, "products")

    geolocation = transform_geolocation(spark, bronze_path)
    
    write_silver_table(geolocation, silver_path, "geolocation")
    
    orders = transform_orders(spark, bronze_path)
    
    write_silver_table(orders, silver_path, "orders")

    order_items = transform_order_items(spark, bronze_path)
    
    write_silver_table(order_items, silver_path, "order_items")
    
    order_payments = transform_order_payments(spark, bronze_path)
    
    write_silver_table(order_payments, silver_path, "order_payments")
    
    order_reviews = transform_order_reviews(spark, bronze_path)
    
    write_silver_table(order_reviews, silver_path, "order_reviews")

    sellers = transform_sellers(spark, bronze_path)
    
    write_silver_table(sellers, silver_path, "sellers")
    
    spark.stop()
    
    