# Databricks notebook source
spark


# COMMAND ----------

storage_account = "bigdatastorageaccount"
application_id = "231df9d0-c832-4872-8c3e-97adf5d27e92"
directory_id = "802279f7-01d4-47da-ba29-91d321bb9114"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "z.m8Q~MC2XAgzZApzg9LL~hWa1tG5hU5HgNbLbM.")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

customer_df = spark.read.\
format("csv")\
.option("header", "true")\
.load(f"abfss://olistdata@bigdatastorageaccount.dfs.core.windows.net/bronze/olist_customers_dataset.csv")

display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # reading data

# COMMAND ----------

# 🔹 List of files you want to read
file_list = [
    "olist_orders_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_products_dataset.csv"
]

# 🔹 Read all files into DataFrames and store in dictionary
dataframes = {}
for file in file_list:
    df_name = file.replace(".csv", "")   # use filename (without extension) as key
    path = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/bronze/{file}"
    
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .load(path))
    
    dataframes[df_name] = df
    print(f"✅ Loaded {file} into dataframe '{df_name}'")

# 🔹 Example: Display each DataFrame
for name, df in dataframes.items():
    print(f"Showing first 5 rows of: {name}")
    display(df.limit(5))   # limit(5) just to preview, remove if you want full data

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading from pymongo

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "f0sj03.h.filess.io"
database = "olistDataNoSQL_tripguide"
port = "27018"
username = "olistDataNoSQL_tripguide"
password = "73f85d6cbb4f82b8996486c7f9c2aded3d173bca"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase


# COMMAND ----------

import pandas as pd
collection = mydatabase['product_category_name_translation']

mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning data

# COMMAND ----------

display(dataframes["olist_products_dataset"])


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, to_date, datediff

orders_df = dataframes["olist_orders_dataset"]

# Step 1: Drop NULL rows
orders_clean = orders_df.na.drop()

# Step 2: Convert string -> timestamp -> date (yyyy-MM-dd only)
orders_transformed = (
    orders_clean
    .withColumn("order_purchase_timestamp", to_date(to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")))
    .withColumn("order_approved_at", to_date(to_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss")))
    .withColumn("order_delivered_carrier_date", to_date(to_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss")))
    .withColumn("order_delivered_customer_date", to_date(to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss")))
    .withColumn("order_estimated_delivery_date", to_date(to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss")))
)

# Step 3: Add delivery_time_days
orders_final = orders_transformed.withColumn(
    "delivery_time_days",
    datediff("order_delivered_customer_date", "order_purchase_timestamp")
)

# Show result
display(orders_final.limit(5))


# COMMAND ----------

#  Drop NULL rows
orders_final = orders_final.na.drop()

# COMMAND ----------

display(orders_final.count())

# COMMAND ----------

# Access the payments dataset
payments_df = dataframes["olist_order_payments_dataset"]

# Remove all rows with NULL values
final_payments = payments_df.na.drop()

# Display cleaned dataframe
display(final_payments.limit(5))

# (Optional) check row count before and after cleaning
print("Before removing NULLs:", payments_df.count())
print("After removing NULLs:", final_payments.count())


# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, upper, to_timestamp, mean, when
from pyspark.sql import functions as F


# COMMAND ----------

reviews = dataframes["olist_order_reviews_dataset"]

reviews = reviews.dropDuplicates() \
                 .withColumn("review_comment_title", trim(col("review_comment_title"))) \
                 .withColumn("review_comment_message", trim(col("review_comment_message"))) \
                 .withColumn("review_creation_date", to_timestamp("review_creation_date")) \
                 .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))

# Handle nulls in comments -> replace with "No Comment"
reviews = reviews.fillna({"review_comment_title": "No Title",
                          "review_comment_message": "No Comment"})

# COMMAND ----------

display(reviews.limit(3))

# COMMAND ----------

items = dataframes["olist_order_items_dataset"]

items = items.dropDuplicates() \
             .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))

# Handle negative/zero values for price & freight (outliers)
items = items.withColumn("price", when(col("price") <= 0, None).otherwise(col("price"))) \
             .withColumn("freight_value", when(col("freight_value") < 0, None).otherwise(col("freight_value")))

# Fill missing prices/freight with average
avg_price = items.select(mean("price")).first()[0]
avg_freight = items.select(mean("freight_value")).first()[0]
items = items.fillna({"price": avg_price, "freight_value": avg_freight})

# COMMAND ----------

display(items.limit(3))

# COMMAND ----------

customers = dataframes["olist_customers_dataset"]

customers = customers.dropDuplicates() \
                     .withColumn("customer_city", lower(trim(col("customer_city")))) \
                     .withColumn("customer_state", upper(trim(col("customer_state"))))

# Handle nulls (drop customers without IDs)
customers = customers.dropna(subset=["customer_id", "customer_unique_id"])


# COMMAND ----------

display(customers.limit(3))

# COMMAND ----------

sellers = dataframes["olist_sellers_dataset"]

sellers = sellers.dropDuplicates() \
                 .withColumn("seller_city", lower(trim(col("seller_city")))) \
                 .withColumn("seller_state", upper(trim(col("seller_state"))))

# Drop rows with missing seller_id
sellers = sellers.dropna(subset=["seller_id"])


# COMMAND ----------

display(sellers.limit(3))

# COMMAND ----------

geo = dataframes["olist_geolocation_dataset"]

geo = geo.dropDuplicates() \
         .withColumn("geolocation_city", lower(trim(col("geolocation_city")))) \
         .withColumn("geolocation_state", upper(trim(col("geolocation_state"))))

# Remove outlier coordinates (filter Brazil valid range)
geo = geo.filter((col("geolocation_lat") >= -33) & (col("geolocation_lat") <= 5)) \
         .filter((col("geolocation_lng") >= -74) & (col("geolocation_lng") <= -34))

# COMMAND ----------

display(geo.limit(3))

# COMMAND ----------

products = dataframes["olist_products_dataset"]

products = products.dropDuplicates() \
                   .withColumn("product_category_name", lower(trim(col("product_category_name"))))

# Handle nulls for product metrics -> replace with mean or mode
for col_name in ["product_name_lenght", "product_description_lenght", 
                 "product_photos_qty", "product_weight_g", 
                 "product_length_cm", "product_height_cm", "product_width_cm"]:
    avg_val = products.select(mean(col_name)).first()[0]
    products = products.withColumn(col_name, when(col(col_name) <= 0, None).otherwise(col(col_name)))
    products = products.fillna({col_name: avg_val})


# COMMAND ----------

display(products.limit(3))

# COMMAND ----------

# Orders + Customers
orders_customers_df = orders_final.join(
    customers, 
    orders_final.customer_id == customers.customer_id,
    "left"
)

# Add Payments
orders_payments_df = orders_customers_df.join(
    final_payments, 
    orders_customers_df.order_id == final_payments.order_id,
    "left"
)

# Add Items
orders_items_df = orders_payments_df.join(
    items, 
    "order_id",   # both have order_id
    "left"
)

# Add Products
orders_items_products_df = orders_items_df.join(
    products, 
    orders_items_df.product_id == products.product_id,
    "left"
)

# Add Sellers (final join)
final_df = orders_items_products_df.join(
    sellers, 
    orders_items_products_df.seller_id == sellers.seller_id,
    "left"
)


# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)
mongo_sparf_df = spark.createDataFrame(mongo_data)
display(mongo_sparf_df)

# COMMAND ----------

final_df = final_df.join(mongo_sparf_df,"product_category_name","left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns

    seen_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)
    
    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistdata@bigdatastorageaccount.dfs.core.windows.net/silver")

# COMMAND ----------

display(final_df)