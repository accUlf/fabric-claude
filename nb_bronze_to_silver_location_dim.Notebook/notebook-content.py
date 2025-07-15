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

# # Bronze to Silver: Location Dimension Transformation
# 
# This notebook transforms NYC taxi location data from the bronze layer into a location dimension table in the silver layer.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math

# Initialize Spark session
spark = SparkSession.builder.appName("LocationDimensionTransformation").getOrCreate()

# CELL ********************

# Read data from bronze layer
bronze_df = spark.read.format("delta").load("abfss://lh_claude_bronze@onelake.dfs.fabric.microsoft.com/Tables/nyctlc")

# Display sample location data
display(bronze_df.select("puLocationId", "doLocationId", "pickupLongitude", "pickupLatitude", 
                        "dropoffLongitude", "dropoffLatitude").limit(10))

# CELL ********************

# Extract pickup locations
pickup_locations = bronze_df.select(
    col("puLocationId").alias("location_id"),
    col("pickupLongitude").alias("longitude"),
    col("pickupLatitude").alias("latitude")
).filter(
    col("location_id").isNotNull() & 
    (col("location_id") != "") &
    col("longitude").isNotNull() & 
    col("latitude").isNotNull()
).distinct()

# Extract dropoff locations
dropoff_locations = bronze_df.select(
    col("doLocationId").alias("location_id"),
    col("dropoffLongitude").alias("longitude"),
    col("dropoffLatitude").alias("latitude")
).filter(
    col("location_id").isNotNull() & 
    (col("location_id") != "") &
    col("longitude").isNotNull() & 
    col("latitude").isNotNull()
).distinct()

# Union and aggregate to get average coordinates for each location
all_locations = pickup_locations.union(dropoff_locations) \
    .groupBy("location_id") \
    .agg(
        avg("longitude").alias("longitude"),
        avg("latitude").alias("latitude"),
        count("*").alias("occurrence_count")
    )

print(f"Total unique locations: {all_locations.count()}")

# CELL ********************

# Define NYC borough boundaries (approximate)
def get_borough(lat, lon):
    # Manhattan approximate boundaries
    if 40.7 <= lat <= 40.9 and -74.02 <= lon <= -73.90:
        return "Manhattan"
    # Brooklyn approximate boundaries
    elif 40.57 <= lat <= 40.74 and -74.05 <= lon <= -73.83:
        return "Brooklyn"
    # Queens approximate boundaries
    elif 40.54 <= lat <= 40.80 and -73.96 <= lon <= -73.70:
        return "Queens"
    # Bronx approximate boundaries
    elif 40.78 <= lat <= 40.92 and -73.93 <= lon <= -73.75:
        return "Bronx"
    # Staten Island approximate boundaries
    elif 40.49 <= lat <= 40.65 and -74.26 <= lon <= -74.05:
        return "Staten Island"
    else:
        return "Other"

# Register UDF
get_borough_udf = udf(get_borough, StringType())

# CELL ********************

# Create location dimension with enhanced attributes
location_dim = all_locations.withColumn(
    "borough", 
    get_borough_udf(col("latitude"), col("longitude"))
).withColumn(
    "zone_type",
    when(col("location_id").cast("int") <= 100, "Airport")
        .when(col("location_id").cast("int").between(101, 200), "Commercial")
        .when(col("location_id").cast("int").between(201, 250), "Residential")
        .otherwise("Mixed")
).withColumn(
    "is_airport",
    when(col("location_id").isin(["1", "2", "132", "138"]), True).otherwise(False)
).withColumn(
    "location_key",
    col("location_id").cast("int")
)

# Add geographic grid references
location_dim = location_dim.withColumn(
    "lat_grid",
    floor((col("latitude") - 40.0) * 100)
).withColumn(
    "lon_grid",
    floor((col("longitude") + 75.0) * 100)
).withColumn(
    "grid_reference",
    concat(col("lat_grid"), lit("_"), col("lon_grid"))
)

# Display sample of the location dimension
display(location_dim.orderBy(col("occurrence_count").desc()).limit(20))

# CELL ********************

# Add distance from city center (Times Square: 40.7580, -73.9855)
times_square_lat = 40.7580
times_square_lon = -73.9855

location_dim_final = location_dim.withColumn(
    "distance_from_center_km",
    round(
        111.111 * sqrt(
            pow(col("latitude") - lit(times_square_lat), 2) + 
            pow((col("longitude") - lit(times_square_lon)) * cos(radians(col("latitude"))), 2)
        ), 2
    )
).withColumn(
    "zone_category",
    when(col("distance_from_center_km") <= 5, "Central")
        .when(col("distance_from_center_km") <= 15, "Inner")
        .when(col("distance_from_center_km") <= 25, "Outer")
        .otherwise("Suburban")
)

# Show schema
location_dim_final.printSchema()

# CELL ********************

# Write location dimension to silver layer
location_dim_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/dim_location")

print(f"Location dimension created with {location_dim_final.count()} records")

# CELL ********************

# Create a view for easy querying
spark.sql("CREATE OR REPLACE TABLE lh_silver.dim_location USING DELTA LOCATION 'Tables/dim_location'")

# Verify the table with some analytics
display(spark.sql("""
    SELECT 
        borough,
        COUNT(*) as location_count,
        AVG(distance_from_center_km) as avg_distance_from_center
    FROM lh_silver.dim_location
    GROUP BY borough
    ORDER BY location_count DESC
"""))

# CELL ********************

# Data quality checks
print("Data Quality Checks:")
print(f"Null location_id values: {location_dim_final.filter(col('location_id').isNull()).count()}")
print(f"Invalid coordinates (0,0): {location_dim_final.filter((col('latitude') == 0) & (col('longitude') == 0)).count()}")
print(f"Locations by borough:")
display(location_dim_final.groupBy("borough").count().orderBy("count", ascending=False))