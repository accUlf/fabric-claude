# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a7b8c9d0-e1f2-3456-7890-bcdef1234567",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze to Silver: Payment, Vendor, and Other Reference Dimensions
# 
# This notebook creates dimension tables for payment types, vendors, rate codes, and trip types from the bronze layer.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("ReferencesDimensionTransformation").getOrCreate()

# CELL ********************

# Read data from bronze layer
bronze_df = spark.read.format("delta").load("abfss://lh_claude_bronze@onelake.dfs.fabric.microsoft.com/Tables/nyctlc")

# Display sample reference data
display(bronze_df.select("paymentType", "vendorID", "rateCodeID", "tripType").limit(20))

# CELL ********************

# Create Payment Type Dimension
payment_data = [
    (1, "Credit Card", "Credit card payment", "Electronic"),
    (2, "Cash", "Cash payment", "Cash"),
    (3, "No Charge", "No charge - ride was free", "Other"),
    (4, "Dispute", "Disputed fare", "Other"),
    (5, "Unknown", "Unknown payment method", "Other"),
    (6, "Voided Trip", "Trip was voided", "Other")
]

payment_schema = StructType([
    StructField("payment_type_id", IntegerType(), False),
    StructField("payment_type_name", StringType(), False),
    StructField("payment_type_description", StringType(), True),
    StructField("payment_category", StringType(), True)
])

payment_dim = spark.createDataFrame(payment_data, payment_schema)

# Add audit columns
payment_dim = payment_dim.withColumn("created_date", current_date()) \
    .withColumn("is_active", lit(True))

display(payment_dim)

# CELL ********************

# Create Vendor Dimension
vendor_data = [
    (1, "Creative Mobile Technologies", "CMT", "Yellow Cab"),
    (2, "VeriFone Inc.", "VTS", "Yellow Cab"),
    (3, "Other", "Other", "Other"),
    (4, "Unknown", "Unknown", "Unknown")
]

vendor_schema = StructType([
    StructField("vendor_id", IntegerType(), False),
    StructField("vendor_name", StringType(), False),
    StructField("vendor_abbreviation", StringType(), True),
    StructField("vendor_type", StringType(), True)
])

vendor_dim = spark.createDataFrame(vendor_data, vendor_schema)

# Add audit columns
vendor_dim = vendor_dim.withColumn("created_date", current_date()) \
    .withColumn("is_active", lit(True))

display(vendor_dim)

# CELL ********************

# Create Rate Code Dimension
rate_code_data = [
    (1, "Standard Rate", "Standard metered fare", 0.0),
    (2, "JFK", "JFK Airport flat fare", 52.0),
    (3, "Newark", "Newark Airport flat fare", 0.0),
    (4, "Nassau or Westchester", "Nassau or Westchester rate", 0.0),
    (5, "Negotiated Fare", "Negotiated flat fare", 0.0),
    (6, "Group Ride", "Group ride rate", 0.0)
]

rate_code_schema = StructType([
    StructField("rate_code_id", IntegerType(), False),
    StructField("rate_code_name", StringType(), False),
    StructField("rate_code_description", StringType(), True),
    StructField("flat_fare_amount", DoubleType(), True)
])

rate_code_dim = spark.createDataFrame(rate_code_data, rate_code_schema)

# Add audit columns
rate_code_dim = rate_code_dim.withColumn("created_date", current_date()) \
    .withColumn("is_airport_rate", when(col("rate_code_id").isin(2, 3), True).otherwise(False))

display(rate_code_dim)

# CELL ********************

# Create Trip Type Dimension
trip_type_data = [
    (1, "Street Hail", "Street hail trip"),
    (2, "Dispatch", "Dispatch trip")
]

trip_type_schema = StructType([
    StructField("trip_type_id", IntegerType(), False),
    StructField("trip_type_name", StringType(), False),
    StructField("trip_type_description", StringType(), True)
])

trip_type_dim = spark.createDataFrame(trip_type_data, trip_type_schema)

# Add audit columns
trip_type_dim = trip_type_dim.withColumn("created_date", current_date()) \
    .withColumn("is_active", lit(True))

display(trip_type_dim)

# CELL ********************

# Write Payment Type dimension to silver layer
payment_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/dim_payment_type")

print(f"Payment Type dimension created with {payment_dim.count()} records")

# CELL ********************

# Write Vendor dimension to silver layer
vendor_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/dim_vendor")

print(f"Vendor dimension created with {vendor_dim.count()} records")

# CELL ********************

# Write Rate Code dimension to silver layer
rate_code_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/dim_rate_code")

print(f"Rate Code dimension created with {rate_code_dim.count()} records")

# CELL ********************

# Write Trip Type dimension to silver layer
trip_type_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/dim_trip_type")

print(f"Trip Type dimension created with {trip_type_dim.count()} records")

# CELL ********************

# Create views for easy querying
spark.sql("CREATE OR REPLACE TABLE lh_silver.dim_payment_type USING DELTA LOCATION 'Tables/dim_payment_type'")
spark.sql("CREATE OR REPLACE TABLE lh_silver.dim_vendor USING DELTA LOCATION 'Tables/dim_vendor'")
spark.sql("CREATE OR REPLACE TABLE lh_silver.dim_rate_code USING DELTA LOCATION 'Tables/dim_rate_code'")
spark.sql("CREATE OR REPLACE TABLE lh_silver.dim_trip_type USING DELTA LOCATION 'Tables/dim_trip_type'")

print("All dimension tables created successfully!")

# CELL ********************

# Verify all dimensions with actual data distribution
print("Payment Type Distribution in Bronze Data:")
display(bronze_df.groupBy("paymentType").count().orderBy("paymentType"))

print("\nVendor Distribution in Bronze Data:")
display(bronze_df.groupBy("vendorID").count().orderBy("vendorID"))

print("\nRate Code Distribution in Bronze Data:")
display(bronze_df.groupBy("rateCodeID").count().orderBy("rateCodeID"))

print("\nTrip Type Distribution in Bronze Data:")
display(bronze_df.groupBy("tripType").count().orderBy("tripType")
