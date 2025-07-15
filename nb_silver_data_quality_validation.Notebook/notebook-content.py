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

# # Silver Layer Data Quality Validation
# 
# This notebook validates the data quality of the silver layer tables after transformation.
# It checks for data consistency, completeness, and business rule compliance.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("SilverDataQualityValidation").getOrCreate()

# CELL ********************

# Define data quality tests
def run_data_quality_tests():
    """Run comprehensive data quality tests on silver layer tables"""
    
    results = {
        "execution_time": datetime.now().isoformat(),
        "tests": []
    }
    
    # Test 1: Check if all dimension tables exist and have data
    dimension_tables = [
        "dim_time", "dim_location", "dim_payment_type", 
        "dim_vendor", "dim_rate_code", "dim_trip_type"
    ]
    
    for table in dimension_tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as count FROM lh_silver.{table}").collect()[0]['count']
            results["tests"].append({
                "test_name": f"Table Exists: {table}",
                "status": "PASS" if count > 0 else "FAIL",
                "value": count,
                "description": f"Table {table} contains {count} records"
            })
        except Exception as e:
            results["tests"].append({
                "test_name": f"Table Exists: {table}",
                "status": "FAIL",
                "value": 0,
                "description": f"Table {table} does not exist or is not accessible: {str(e)}"
            })
    
    return results

# Run initial tests
quality_results = run_data_quality_tests()

# CELL ********************

# Test 2: Fact table data quality checks
def validate_fact_table():
    """Validate fact_trips table data quality"""
    
    fact_tests = []
    
    try:
        # Check fact table exists and has data
        fact_count = spark.sql("SELECT COUNT(*) as count FROM lh_silver.fact_trips").collect()[0]['count']
        fact_tests.append({
            "test_name": "Fact Table Record Count",
            "status": "PASS" if fact_count > 0 else "FAIL",
            "value": fact_count,
            "description": f"Fact table contains {fact_count} records"
        })
        
        # Check for null keys in fact table
        null_checks = spark.sql("""
            SELECT 
                SUM(CASE WHEN pickup_time_key IS NULL THEN 1 ELSE 0 END) as null_pickup_time,
                SUM(CASE WHEN dropoff_time_key IS NULL THEN 1 ELSE 0 END) as null_dropoff_time,
                SUM(CASE WHEN pickup_location_key IS NULL THEN 1 ELSE 0 END) as null_pickup_location,
                SUM(CASE WHEN dropoff_location_key IS NULL THEN 1 ELSE 0 END) as null_dropoff_location
            FROM lh_silver.fact_trips
        """).collect()[0]
        
        for field_name in null_checks.__fields__:
            null_count = null_checks[field_name]
            fact_tests.append({
                "test_name": f"Null Key Check: {field_name}",
                "status": "PASS" if null_count == 0 else "FAIL",
                "value": null_count,
                "description": f"Found {null_count} null values in {field_name}"
            })
        
        # Check for reasonable trip durations
        duration_stats = spark.sql("""
            SELECT 
                MIN(trip_duration_minutes) as min_duration,
                MAX(trip_duration_minutes) as max_duration,
                AVG(trip_duration_minutes) as avg_duration,
                COUNT(*) as total_trips
            FROM lh_silver.fact_trips
        """).collect()[0]
        
        fact_tests.append({
            "test_name": "Trip Duration Range",
            "status": "PASS" if 0 < duration_stats['min_duration'] < 480 and duration_stats['max_duration'] < 480 else "FAIL",
            "value": f"Min: {duration_stats['min_duration']}, Max: {duration_stats['max_duration']}, Avg: {round(duration_stats['avg_duration'], 2)}",
            "description": "Trip durations should be between 0 and 480 minutes"
        })
        
        # Check for reasonable financial amounts
        amount_stats = spark.sql("""
            SELECT 
                MIN(total_amount) as min_amount,
                MAX(total_amount) as max_amount,
                AVG(total_amount) as avg_amount,
                SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) as negative_amounts
            FROM lh_silver.fact_trips
        """).collect()[0]
        
        fact_tests.append({
            "test_name": "Financial Amount Validation",
            "status": "PASS" if amount_stats['negative_amounts'] == 0 and amount_stats['max_amount'] < 1000 else "FAIL",
            "value": f"Min: ${amount_stats['min_amount']}, Max: ${amount_stats['max_amount']}, Negative: {amount_stats['negative_amounts']}",
            "description": "Total amounts should be positive and reasonable"
        })
        
    except Exception as e:
        fact_tests.append({
            "test_name": "Fact Table Validation",
            "status": "FAIL",
            "value": 0,
            "description": f"Error validating fact table: {str(e)}"
        })
    
    return fact_tests

# Add fact table tests to results
quality_results["tests"].extend(validate_fact_table())

# CELL ********************

# Test 3: Referential integrity checks
def validate_referential_integrity():
    """Check referential integrity between fact and dimension tables"""
    
    integrity_tests = []
    
    try:
        # Check orphaned records in fact table
        orphaned_checks = spark.sql("""
            SELECT 
                COUNT(*) as total_facts,
                SUM(CASE WHEN t.time_key IS NULL THEN 1 ELSE 0 END) as orphaned_pickup_time,
                SUM(CASE WHEN t2.time_key IS NULL THEN 1 ELSE 0 END) as orphaned_dropoff_time,
                SUM(CASE WHEN l.location_key IS NULL THEN 1 ELSE 0 END) as orphaned_pickup_location,
                SUM(CASE WHEN l2.location_key IS NULL THEN 1 ELSE 0 END) as orphaned_dropoff_location
            FROM lh_silver.fact_trips f
            LEFT JOIN lh_silver.dim_time t ON f.pickup_time_key = t.time_key
            LEFT JOIN lh_silver.dim_time t2 ON f.dropoff_time_key = t2.time_key
            LEFT JOIN lh_silver.dim_location l ON f.pickup_location_key = l.location_key
            LEFT JOIN lh_silver.dim_location l2 ON f.dropoff_location_key = l2.location_key
        """).collect()[0]
        
        total_facts = orphaned_checks['total_facts']
        
        for field_name in orphaned_checks.__fields__:
            if field_name != 'total_facts':
                orphaned_count = orphaned_checks[field_name]
                integrity_tests.append({
                    "test_name": f"Referential Integrity: {field_name}",
                    "status": "PASS" if orphaned_count == 0 else "FAIL",
                    "value": f"{orphaned_count}/{total_facts}",
                    "description": f"Found {orphaned_count} orphaned records out of {total_facts} total facts"
                })
        
    except Exception as e:
        integrity_tests.append({
            "test_name": "Referential Integrity Check",
            "status": "FAIL",
            "value": 0,
            "description": f"Error checking referential integrity: {str(e)}"
        })
    
    return integrity_tests

# Add referential integrity tests
quality_results["tests"].extend(validate_referential_integrity())

# CELL ********************

# Test 4: Business rule validation
def validate_business_rules():
    """Validate business-specific rules"""
    
    business_tests = []
    
    try:
        # Check for logical inconsistencies
        business_checks = spark.sql("""
            SELECT 
                COUNT(*) as total_trips,
                SUM(CASE WHEN pickup_time_key >= dropoff_time_key THEN 1 ELSE 0 END) as invalid_time_sequence,
                SUM(CASE WHEN pickup_location_key = dropoff_location_key THEN 1 ELSE 0 END) as same_location_trips,
                SUM(CASE WHEN trip_distance = 0 AND total_amount > 0 THEN 1 ELSE 0 END) as zero_distance_paid_trips,
                SUM(CASE WHEN tip_amount > fare_amount THEN 1 ELSE 0 END) as tip_exceeds_fare
            FROM lh_silver.fact_trips
        """).collect()[0]
        
        total_trips = business_checks['total_trips']
        
        business_tests.append({
            "test_name": "Time Sequence Validation",
            "status": "PASS" if business_checks['invalid_time_sequence'] == 0 else "WARN",
            "value": f"{business_checks['invalid_time_sequence']}/{total_trips}",
            "description": f"Found {business_checks['invalid_time_sequence']} trips with pickup time >= dropoff time"
        })
        
        business_tests.append({
            "test_name": "Same Location Trips",
            "status": "PASS" if business_checks['same_location_trips'] < total_trips * 0.05 else "WARN",
            "value": f"{business_checks['same_location_trips']}/{total_trips}",
            "description": f"Found {business_checks['same_location_trips']} trips with same pickup/dropoff location"
        })
        
        business_tests.append({
            "test_name": "Zero Distance Paid Trips",
            "status": "PASS" if business_checks['zero_distance_paid_trips'] < total_trips * 0.01 else "WARN",
            "value": f"{business_checks['zero_distance_paid_trips']}/{total_trips}",
            "description": f"Found {business_checks['zero_distance_paid_trips']} trips with zero distance but payment"
        })
        
        # Check data freshness
        data_freshness = spark.sql("""
            SELECT 
                MAX(pickup_datetime) as latest_pickup,
                MIN(pickup_datetime) as earliest_pickup,
                DATEDIFF(CURRENT_DATE(), MAX(DATE(pickup_datetime))) as days_since_latest
            FROM lh_silver.fact_trips
        """).collect()[0]
        
        business_tests.append({
            "test_name": "Data Freshness",
            "status": "PASS" if data_freshness['days_since_latest'] <= 7 else "WARN",
            "value": f"{data_freshness['days_since_latest']} days",
            "description": f"Latest data is {data_freshness['days_since_latest']} days old"
        })
        
    except Exception as e:
        business_tests.append({
            "test_name": "Business Rules Validation",
            "status": "FAIL",
            "value": 0,
            "description": f"Error validating business rules: {str(e)}"
        })
    
    return business_tests

# Add business rule tests
quality_results["tests"].extend(validate_business_rules())

# CELL ********************

# Generate final report
def generate_quality_report(results):
    """Generate a comprehensive data quality report"""
    
    total_tests = len(results["tests"])
    passed_tests = sum(1 for test in results["tests"] if test["status"] == "PASS")
    failed_tests = sum(1 for test in results["tests"] if test["status"] == "FAIL")
    warning_tests = sum(1 for test in results["tests"] if test["status"] == "WARN")
    
    print("=" * 80)
    print("SILVER LAYER DATA QUALITY REPORT")
    print("=" * 80)
    print(f"Execution Time: {results['execution_time']}")
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests} ({passed_tests/total_tests*100:.1f}%)")
    print(f"Failed: {failed_tests} ({failed_tests/total_tests*100:.1f}%)")
    print(f"Warnings: {warning_tests} ({warning_tests/total_tests*100:.1f}%)")
    print("=" * 80)
    
    # Print detailed results
    for test in results["tests"]:
        status_symbol = "✓" if test["status"] == "PASS" else "✗" if test["status"] == "FAIL" else "⚠"
        print(f"{status_symbol} {test['test_name']}: {test['value']}")
        if test["status"] != "PASS":
            print(f"   Description: {test['description']}")
    
    print("=" * 80)
    
    # Determine overall status
    if failed_tests > 0:
        print("OVERALL STATUS: FAILED")
        return False
    elif warning_tests > 0:
        print("OVERALL STATUS: PASSED WITH WARNINGS")
        return True
    else:
        print("OVERALL STATUS: PASSED")
        return True

# Generate and display report
overall_success = generate_quality_report(quality_results)

# CELL ********************

# Save results to a table for monitoring
def save_quality_results(results, success_status):
    """Save quality results for monitoring and trending"""
    
    try:
        # Create a summary record
        summary_data = [{
            "execution_time": results["execution_time"],
            "total_tests": len(results["tests"]),
            "passed_tests": sum(1 for test in results["tests"] if test["status"] == "PASS"),
            "failed_tests": sum(1 for test in results["tests"] if test["status"] == "FAIL"),
            "warning_tests": sum(1 for test in results["tests"] if test["status"] == "WARN"),
            "overall_status": "PASSED" if success_status else "FAILED",
            "execution_date": datetime.now().date().isoformat()
        }]
        
        summary_df = spark.createDataFrame(summary_data)
        
        # Write to silver layer for monitoring
        summary_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save("Tables/data_quality_summary")
        
        # Create table if it doesn't exist
        spark.sql("CREATE TABLE IF NOT EXISTS lh_silver.data_quality_summary USING DELTA LOCATION 'Tables/data_quality_summary'")
        
        print("Quality results saved successfully!")
        
    except Exception as e:
        print(f"Error saving quality results: {str(e)}")

# Save results
save_quality_results(quality_results, overall_success)

# CELL ********************

# Final validation - fail the notebook if critical tests failed
if not overall_success:
    failed_critical_tests = [test for test in quality_results["tests"] if test["status"] == "FAIL"]
    error_message = f"Data quality validation failed. {len(failed_critical_tests)} critical tests failed."
    print(f"ERROR: {error_message}")
    raise Exception(error_message)
else:
    print("Data quality validation completed successfully!")
    print("Silver layer is ready for consumption.")