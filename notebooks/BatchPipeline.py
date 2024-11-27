#!/usr/bin/env python
# coding: utf-8

# In[13]:


from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, sum as _sum, dense_rank, round as _round
from pyspark.sql.window import Window


# In[14]:


# Spark Configuration
sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("Best_Selling_Products_Pipeline")
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


# In[15]:


# Load Fact and Dimension Tables
factDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/fact_table.csv")
itemDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/item_dim.csv")
storeDF = spark.read.format("csv").option("header", "true").load(f"gs://{data_bucket_uri}/store_dim.csv")


# In[22]:


# Select Relevant Columns
factDF = factDF.select("item_key", "store_key", "total_price")
itemDF = itemDF.select("item_key", "item_name")
storeDF = storeDF.select("store_key")

# Join Fact Table with Item and Store Dimension Tables
joinedDF = factDF.join(itemDF, "item_key").join(storeDF, "store_key")

# Step 1: Calculate Total Sales per Product per Store
storeProductSalesDF = joinedDF.groupBy("store_key", "item_key", "item_name") \
    .agg(_sum("total_price").alias("product_sales"))

# Step 2: Calculate Total Store Revenue
storeTotalSalesDF = storeProductSalesDF.groupBy("store_key") \
    .agg(_sum("product_sales").alias("store_total_sales"))

# Step 3: Rank Stores Based on Total Sales
storeRankWindow = Window.orderBy(col("store_total_sales").desc())
storeTotalSalesDF = storeTotalSalesDF.withColumn("store_rank", dense_rank().over(storeRankWindow))

# Step 4: Add Dense Rank for Products within Each Store
productRankWindow = Window.partitionBy("store_key").orderBy(col("product_sales").desc())
storeProductSalesDF = storeProductSalesDF.withColumn("product_rank", dense_rank().over(productRankWindow))

# Step 5: Remove Products with no Sales
storeProductSalesDF = storeProductSalesDF.where(col("product_sales") > 0)

# Step 6: Filter to Keep Only the Top 100 Stores
top100StoresDF = storeTotalSalesDF.where(col("store_rank") <= 100) \
    .withColumnRenamed("store_total_sales", "top_store_total_sales") \
    .withColumnRenamed("store_rank", "top_store_rank")

# Step 7: Retain Product-Level Details for Top 100 Stores
finalDF = storeProductSalesDF.join(top100StoresDF, "store_key")

# Show Results
finalDF.show()


# In[23]:


# Save to BigQuery
spark.conf.set('temporaryGcsBucket', temp_bucket)

finalDF.write.format('bigquery') \
    .option('table', f'{project_id}.a2.product_sales_by_store') \
    .mode("overwrite") \
    .save()

# Stop Spark Session
spark.stop()

