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

# # Bronze to Silver: Time Dimension Transformation
# 
# This notebook transforms NYC taxi trip datetime data from the bronze layer into a time dimension table in the silver layer.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("TimeDimensionTransformation").getOrCreate()

# CELL ********************

# Read data from bronze layer
bronze_df = spark.read.format("delta").load("abfss://lh_claude_bronze@onelake.dfs.fabric.microsoft.com/Tables/nyctlc")

# Display sample data
display(bronze_df.select("lpepPickupDatetime", "lpepDropoffDatetime").limit(10))

# CELL ********************

# Extract unique datetime values from both pickup and dropoff times
pickup_times = bronze_df.select(col("lpepPickupDatetime").alias("datetime")).filter(col("datetime").isNotNull())
dropoff_times = bronze_df.select(col("lpepDropoffDatetime").alias("datetime")).filter(col("datetime").isNotNull())

# Union and get distinct datetime values
all_times = pickup_times.union(dropoff_times).distinct()

print(f"Total unique datetime values: {all_times.count()}")

# CELL ********************

# Create time dimension with various attributes
time_dim = all_times.select(
    col("datetime"),
    # Date components
    to_date(col("datetime")).alias("date"),
    year(col("datetime")).alias("year"),
    quarter(col("datetime")).alias("quarter"),
    month(col("datetime")).alias("month"),
    dayofmonth(col("datetime")).alias("day"),
    dayofweek(col("datetime")).alias("dayofweek"),
    weekofyear(col("datetime")).alias("weekofyear"),
    
    # Time components
    hour(col("datetime")).alias("hour"),
    minute(col("datetime")).alias("minute"),
    
    # Derived attributes
    date_format(col("datetime"), "EEEE").alias("dayname"),
    date_format(col("datetime"), "MMMM").alias("monthname"),
    date_format(col("datetime"), "yyyy-MM").alias("yearmonth"),
    
    # Business attributes
    when(dayofweek(col("datetime")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
    when(hour(col("datetime")).between(6, 9), "Morning Rush")
        .when(hour(col("datetime")).between(10, 15), "Midday")
        .when(hour(col("datetime")).between(16, 19), "Evening Rush")
        .when(hour(col("datetime")).between(20, 23), "Night")
        .otherwise("Late Night").alias("time_period"),
    
    # Create a surrogate key
    date_format(col("datetime"), "yyyyMMddHHmm").cast("bigint").alias("time_key")
).distinct()

# Display sample of the time dimension
display(time_dim.limit(20))

# CELL ********************

# Add additional business logic columns
time_dim_final = time_dim.withColumn(
    "is_holiday", 
    when(
        ((col("month") == 1) & (col("day") == 1)) |  # New Year's Day
        ((col("month") == 7) & (col("day") == 4)) |  # Independence Day
        ((col("month") == 12) & (col("day") == 25)) |  # Christmas
        ((col("month") == 11) & (col("day").between(22, 28)) & (col("dayofweek") == 5)) |  # Thanksgiving (4th Thursday of November)
        ((col("month") == 5) & (col("day").between(25, 31)) & (col("dayofweek") == 2))  # Memorial Day (Last Monday of May)
        , True
    ).otherwise(False)
).withColumn(
    "fiscal_quarter",
    when(col("month").isin([1, 2, 3]), "Q1")
        .when(col("month").isin([4, 5, 6]), "Q2")
        .when(col("month").isin([7, 8, 9]), "Q3")
        .otherwise("Q4")
)

# Show schema
time_dim_final.printSchema()

# CELL ********************

# Write time dimension to silver layer
time_dim_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/dim_time")

print(f"Time dimension created with {time_dim_final.count()} records")

# CELL ********************

# Create a view for easy querying
spark.sql("CREATE OR REPLACE TABLE lh_silver.dim_time USING DELTA LOCATION 'Tables/dim_time'")

# Verify the table
display(spark.sql("SELECT * FROM lh_silver.dim_time ORDER BY datetime DESC LIMIT 10"))

# CELL ********************

# Data quality checks
print("Data Quality Checks:")
print(f"Null datetime values: {time_dim_final.filter(col('datetime').isNull()).count()}")
print(f"Duplicate time_key values: {time_dim_final.groupBy('time_key').count().filter(col('count') > 1).count()}")
print(f"Date range: {time_dim_final.agg(min('datetime'), max('datetime')).collect()[0]}"
