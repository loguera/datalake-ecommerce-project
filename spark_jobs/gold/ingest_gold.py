import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, year, month, dayofmonth

def create_gold_schema():
    
    spark = SparkSession.builder.appName("Gold").getOrCreate()
    
    current_path = os.path.dirname(os.path.abspath(__file__))
    bronze_path = os.path.join(current_path, "..", "..", "data", "bronze")
    silver_path = os.path.join(current_path, "..", "..", "data", "silver")
    gold_path = os.path.join(current_path, "..", "..", "data", "gold")
    
    try:
        
        orders_df = spark.read.parquet(os.path.join(bronze_path, "orders"))
        order_items_df = spark.read.parquet(os.path.join(bronze_path, "order_items"))
        products_df = spark.read.parquet(os.path.join(bronze_path, "products"))
        customers_df = spark.read.parquet(os.path.join(bronze_path, "customers"))
        geolocation_df = spark.read.parquet(os.path.join(bronze_path, "geolocation"))      
        
    except Exception as e:
        print(f'Error: {e}')
        return
    
    
    dm_customers = customers_df.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state")
    ).distinct()
    
    dm_products = products_df.select(
        col("product_id"),
        col("product_category_name"),
        col("product_name_lenght"),
        col("product_description_lenght"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    ).distinct()
    
    dm_geolocation = geolocation_df.select(
        col("geolocation_zip_code_prefix"),
        col("geolocation_lat"),
        col("geolocation_lng"),
        col("geolocation_city"),
        col("geolocation_state")
    ).distinct()
    
    ft_orders = orders_df.join(order_items_df, ('order_id')).select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("order_status"),
        col("price"),
        to_timestamp(col("order_purchase_timestamp"), "YYYY-MM-dd").alias("order_purchase_timestamp")
    )
    
    ft_orders = ft_orders.withColumn("order_purchase_year", year(col("order_purchase_timestamp"))).withColumn("order_purchase_month", month(col("order_purchase_timestamp"))).withColumn("order_purchase_day", dayofmonth(col("order_purchase_timestamp")))
    
    try:
        dm_customers.write.mode("overwrite").parquet(os.path.join(gold_path, "dm_customers"))
        dm_products.write.mode("overwrite").parquet(os.path.join(gold_path, "dm_products"))
        dm_geolocation.write.mode("overwrite").parquet(os.path.join(gold_path, "dm_geolocation"))
        ft_orders.write.mode("overwrite").parquet(os.path.join(gold_path, "ft_orders"))
        
        print("Gold layer created!")
        
    except Exception as e:
        print("Error: {e}")
        
    spark.stop()
    

if __name__ == "__main__":
    create_gold_schema()