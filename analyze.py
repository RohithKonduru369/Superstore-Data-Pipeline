import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Analyze").getOrCreate()
df = spark.read.csv("superstore.csv", header=True)
df=df.withColumn("Order_date",F.col("Order_Date").cast("date"))
df=df.withColumn("sales",F.col("Sales").cast("float"))
# df.printSchema()
df.fillna(0)
df.printSchema()
#Data Schema Modernization (Star Schema Design)
dim_customer=df.select("Customer_id","Customer_Name","Segment").distinct()
dim_product=df.select("Product_ID","Product_Name","Category","Sub_Category").distinct()
dim_order=df.select("Order_id","Order_date","Ship_Date","Ship_Mode","Product_ID","Customer_id","Postal_Code","Sales","Quantity","Discount","Profit")
dim_geo=df.select("State","Country","City","Region","Postal_Code").distinct()

dim_customer.write.mode("overwrite").parquet("dim_customers.parquet")
dim_product.write.mode("overwrite").parquet("dim_products.parquet")
dim_order.write.mode("overwrite").parquet("dim_orders.parquet")
dim_geo.write.mode("overwrite").parquet("dim_geos.parquet")

dim_customer.show()
dim_product.show()  
dim_order.show()
dim_geo.show()
#------------------------------------------------------
#Automated Data Quality & Validation Framework
cleaned_df=df.withColumn("is_bad",(F.col("sales").isNull()) | (F.col("Ship_Date") < F.col("Order_date")))
Valued_df=cleaned_df.filter(F.col("is_bad") == False)
Valued_df.printSchema()
Valued_df.show()
#-----------------------------------------------------
#Incremental Load & Deduplication Logic
df=df.withColumn("processing_time", F.current_timestamp())
existing_data= df.limit(100)
new_batch_df =existing_data.limit(10).withColumn("processing_time", F.date_add(F.current_timestamp(), 1))
combined_df = existing_data.union(new_batch_df)
combined_df.show()
window_spec = W.partitionBy("Order_id", "Product_ID").orderBy(F.col("processing_time").desc())
upserted_df = combined_df.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1)\
                         .drop("row_num")
upserted_df.select("Order_id", "Product_ID", "processing_time").show()
#------------------------------------------------------
# Feature Engineering Pipeline
# Using Spark SQL functions for high-performance transformations
enriched_df = df.withColumn("Shipping_Days", F.datediff(F.col("Ship_Date"), F.col("Order_Date"))) \
    .withColumn("Order_Month", F.month(F.col("Order_Date"))) \
    .withColumn("Order_Quarter", F.quarter(F.col("Order_Date"))) \
    .withColumn("Profit_Margin", (F.col("Profit") / F.col("sales")) * 100) \
    .withColumn("Is_Late", F.when(F.col("Shipping_Days") > 5, True).otherwise(False))

# Check the results
enriched_df.select("Order_id", "Shipping_Days", "Profit_Margin", "Is_Late").show(5)