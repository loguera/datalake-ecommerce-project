from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os

def main():
    spark = SparkSession.builder.appName("Ingest Silver").getOrCreate()
    
    current_path = os.path.dirname(os.path.abspath(__file__))
    bronze_path = os.path.join(current_path, "..", "..", "data", "bronze")
    silver_path = os.path.join(current_path, "..", "..", "data", "silver")
    
    products_df = spark.read.parquet(os.path.join(bronze_path, "products"))
    products_df.write.mode("overwrite").parquet(os.path.join(silver_path, "products"))
    
    customers_df = spark.read.parquet(os.path.join(bronze_path, "customers"))
    customers_df.write.mode("overwrite").parquet(os.path.join(silver_path, "customers"))
    
    geolocation_df = spark.read.parquet(os.path.join(bronze_path, "geolocation"))
    geolocation_df.write.mode("overwrite").parquet(os.path.join(silver_path, "geolocation"))
    
    orders_df = spark.read.parquet(os.path.join(bronze_path, "orders"))
    orders_df = orders_df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
    orders_df.write.mode("overwrite").parquet(os.path.join(silver_path, "orders"))
    
    order_items_df = spark.read.parquet(os.path.join(bronze_path, "order_items"))
    order_items_df.write.mode("overwrite").parquet(os.path.join(silver_path, "order_items"))
    
    spark.stop()
    
    
if __name__=="__main__":
    main()