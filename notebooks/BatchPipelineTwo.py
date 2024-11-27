#!/usr/bin/env python
# coding: utf-8

# In[30]:


from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, sum as _sum, dense_rank, round as _round
from pyspark.sql.window import Window


# In[2]:


# Spark Configuration
sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("Top_Categories_Products_Pipeline")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

data_bucket_uri = "data_de2024_a2"
temp_bucket = "temp_de2024_mh"
project_id = "core-synthesis-435410-v9"


# In[3]:


# Load Fact and Dimension Tables
factDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/fact_table.csv")
itemDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/item_dim.csv")
storeDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/store_dim.csv")
transDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/Trans_dim.csv")
timeDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/time_dim.csv")
customerDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/customer_dim.csv")


# In[4]:


# Drop eventual duplicates
itemDF_cleaned = itemDF.drop("unit_price", "unit_price")
# Join Fact Table with Item and Store Dimension Tables
joinedDF = factDF.join(itemDF_cleaned, "item_key").join(storeDF, "store_key").join(timeDF, "time_key").join(transDF, "payment_key").join(customerDF, "coustomer_key")
joinedDF.show()


# In[39]:


# Step 1: Aggregate sales, quantity, and calculate average price per item
store_category_sales = joinedDF.groupBy("store_key", "desc").agg(
    _sum("total_price").alias("category_sales"),
    _sum("quantity").alias("total_quantity")
).withColumn(
    "avg_price_per_item", _round(col("category_sales") / col("total_quantity"))
)

# Step 2: Calculate total sales per store
store_total_sales = joinedDF.groupBy("store_key").agg(
    _sum("total_price").alias("store_sales")
)

# Step 3: Join and calculate contribution percentage
store_category_contribution = store_category_sales.join(
    store_total_sales, on="store_key"
).withColumn(
    "contribution_pct", _round((col("category_sales") / col("store_sales")) * 100,2)
)

# Step 4: Identify the top-contributing category for each store using dense rank
window_spec = Window.partitionBy("store_key").orderBy(col("category_sales").desc())

top_category_per_store = store_category_contribution.withColumn(
    "rank", dense_rank().over(window_spec)
).filter(col("rank") == 1).select(
    "store_key", "desc", "category_sales", "contribution_pct", "avg_price_per_item"
)

top_category_per_store.show()


# In[40]:


# Save to BigQuery
spark.conf.set('temporaryGcsBucket', temp_bucket)

top_category_per_store.write.format('bigquery') \
    .option('table', f'{project_id}.a2.top_product_categories_by_store') \
    .mode("overwrite") \
    .save()

# Stop Spark Session
spark.stop()

