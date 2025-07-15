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

# # Bronze to Silver: Fact Trips Transformation
# 
# This notebook transforms NYC taxi trip data from the bronze layer into a fact table in the silver layer,
# connecting to all dimension tables created previously.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("FactTripsTransformation").getOrCreate()

# CELL ********************

# Read data from bronze layer
bronze_df = spark.read.format("delta").load("abfss://lh_claude_bronze@onelake.dfs.fabric.microsoft.com/Tables/nyctlc")

print(f"Total records in bronze layer: {bronze_df.count()}")
bronze_df.printSchema()

# CELL ********************

# Read dimension tables to get keys
dim_time = spark.read.format("delta").load("Tables/dim_time")
dim_location = spark.read.format("delta").load("Tables/dim_location")
dim_payment = spark.read.format("delta").load("Tables/dim_payment_type")
dim_vendor = spark.read.format("delta").load("Tables/dim_vendor")
dim_rate_code = spark.read.format("delta").load("Tables/dim_rate_code")
dim_trip_type = spark.read.format("delta").load("Tables/dim_trip_type")

# CELL ********************

# Create fact table with surrogate keys from dimensions
fact_trips = bronze_df

# Join with time dimension for pickup time
fact_trips = fact_trips.join(
    dim_time.select(col("datetime"), col("time_key").alias("pickup_time_key")),
    fact_trips.lpepPickupDatetime == dim_time.datetime,
    "left"
)

# Join with time dimension for dropoff time
fact_trips = fact_trips.join(
    dim_time.select(col("datetime"), col("time_key").alias("dropoff_time_key")),
    fact_trips.lpepDropoffDatetime == dim_time.datetime,
    "left"
)

# Join with location dimensions
fact_trips = fact_trips.join(
    dim_location.select(col("location_id"), col("location_key").alias("pickup_location_key")),
    fact_trips.puLocationId == dim_location.location_id,
    "left"
)

fact_trips = fact_trips.join(
    dim_location.select(col("location_id"), col("location_key").alias("dropoff_location_key")),
    fact_trips.doLocationId == dim_location.location_id,
    "left"
)

# CELL ********************

# Continue with other dimension joins and create calculated measures
fact_trips_transformed = fact_trips \
    .withColumn("trip_duration_minutes", 
        round((unix_timestamp("lpepDropoffDatetime") - unix_timestamp("lpepPickupDatetime")) / 60, 2)) \
    .withColumn("speed_mph", 
        when(col("trip_duration_minutes") > 0, 
             round(col("tripDistance") / (col("trip_duration_minutes") / 60), 2))
        .otherwise(0)) \
    .withColumn("cost_per_mile", 
        when(col("tripDistance") > 0, round(col("totalAmount") / col("tripDistance"), 2))
        .otherwise(0)) \
    .withColumn("tip_percentage", 
        when(col("fareAmount") > 0, round((col("tipAmount") / col("fareAmount")) * 100, 2))
        .otherwise(0))

# CELL ********************

# Select final columns for fact table
fact_trips_final = fact_trips_transformed.select(
    # Surrogate keys
    col("pickup_time_key"),
    col("dropoff_time_key"),
    col("pickup_location_key"),
    col("dropoff_location_key"),
    col("vendorID").alias("vendor_key"),
    col("paymentType").alias("payment_type_key"),
    col("rateCodeID").alias("rate_code_key"),
    col("tripType").alias("trip_type_key"),
    
    # Degenerate dimensions
    col("storeAndFwdFlag").alias("store_and_fwd_flag"),
    
    # Measures
    col("passengerCount").alias("passenger_count"),
    col("tripDistance").alias("trip_distance"),
    col("trip_duration_minutes"),
    col("speed_mph"),
    
    # Financial measures
    col("fareAmount").alias("fare_amount"),
    col("extra"),
    col("mtaTax").alias("mta_tax"),
    col("improvementSurcharge").alias("improvement_surcharge"),
    col("tipAmount").alias("tip_amount"),
    col("tollsAmount").alias("tolls_amount"),
    col("ehailFee").alias("ehail_fee"),
    col("totalAmount").alias("total_amount"),
    col("cost_per_mile"),
    col("tip_percentage"),
    
    # Original datetime columns for reference
    col("lpepPickupDatetime").alias("pickup_datetime"),
    col("lpepDropoffDatetime").alias("dropoff_datetime")
).filter(
    # Basic data quality filters
    col("pickup_time_key").isNotNull() & 
    col("dropoff_time_key").isNotNull() &
    (col("trip_duration_minutes") > 0) &
    (col("trip_duration_minutes") < 480) &  # Less than 8 hours
    (col("trip_distance") > 0) &
    (col("trip_distance") < 100) &  # Less than 100 miles
    (col("total_amount") > 0) &
    (col("total_amount") < 1000)  # Less than $1000
)

# CELL ********************

# Add a unique trip ID
window_spec = Window.orderBy("pickup_datetime", "dropoff_datetime", "pickup_location_key")
fact_trips_final = fact_trips_final.withColumn("trip_id", row_number().over(window_spec))

# Rearrange columns with trip_id first
fact_trips_final = fact_trips_final.select("trip_id", *[col for col in fact_trips_final.columns if col != "trip_id"])

print(f"Fact table records after filtering: {fact_trips_final.count()}")
fact_trips_final.printSchema()

# CELL ********************

# Write fact table to silver layer
fact_trips_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/fact_trips")

print(f"Fact trips table created with {fact_trips_final.count()} records")

# CELL ********************

# Create a view for easy querying
spark.sql("CREATE OR REPLACE TABLE lh_silver.fact_trips USING DELTA LOCATION 'Tables/fact_trips'")

# Verify the fact table with some analytics
display(spark.sql("""
    SELECT 
        DATE(pickup_datetime) as trip_date,
        COUNT(*) as trip_count,
        ROUND(AVG(trip_distance), 2) as avg_distance,
        ROUND(AVG(total_amount), 2) as avg_amount,
        ROUND(AVG(trip_duration_minutes), 2) as avg_duration_minutes
    FROM lh_silver.fact_trips
    GROUP BY DATE(pickup_datetime)
    ORDER BY trip_date DESC
    LIMIT 30
"""))

# CELL ********************

# Data quality summary
print("Fact Table Quality Summary:")
print("=" * 50)

quality_metrics = spark.sql("""
    SELECT 
        COUNT(*) as total_trips,
        COUNT(DISTINCT trip_id) as unique_trips,
        COUNT(DISTINCT pickup_time_key) as unique_pickup_times,
        COUNT(DISTINCT pickup_location_key) as unique_pickup_locations,
        COUNT(DISTINCT payment_type_key) as unique_payment_types,
        MIN(pickup_datetime) as earliest_trip,
        MAX(pickup_datetime) as latest_trip,
        ROUND(AVG(trip_distance), 2) as avg_trip_distance,
        ROUND(AVG(total_amount), 2) as avg_trip_amount
    FROM lh_silver.fact_trips
""").collect()[0]

for col_name in quality_metrics.__fields__:
    print(f"{col_name}: {quality_metrics[col_name]}")

# CELL ********************

# Create summary statistics by dimension
print("\nTrip counts by Payment Type:")
display(spark.sql("""
    SELECT 
        payment_type_key,
        COUNT(*) as trip_count,
        ROUND(AVG(total_amount), 2) as avg_amount
    FROM lh_silver.fact_trips
    GROUP BY payment_type_key
    ORDER BY trip_count DESC
"""))

print("\nTrip counts by Vendor:")
display(spark.sql("""
    SELECT 
        vendor_key,
        COUNT(*) as trip_count,
        ROUND(AVG(trip_distance), 2) as avg_distance
    FROM lh_silver.fact_trips
    GROUP BY vendor_key
    ORDER BY trip_count DESC
"""))