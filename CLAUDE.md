# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Microsoft Fabric data engineering project implementing a medallion architecture for NYC Taxi data processing. The project uses PySpark for transformations and integrates AI capabilities for semantic analysis.

## Architecture

The data flows through these layers:
1. **External Data** → NYC Taxi data from Azure Open Data Storage
2. **Bronze Layer** (`lh_claude_bronze`) → Raw data in Delta Lake format
3. **Transformation Layer** → PySpark notebooks for ETL
4. **Silver Layer** (`lh_silver`) → Dimensional model (fact and dimension tables)
5. **Semantic Layer** → Business intelligence model for Power BI

## Key Components and Dependencies

### Data Processing Order
When executing the pipeline manually, follow this sequence:
1. **Data Ingestion**: `pipe_nyTaxi_to_bronze` pipeline
2. **Parallel Dimension Processing**:
   - `nb_bronze_to_silver_time_dim`
   - `nb_bronze_to_silver_location_dim`
   - `nb_bronze_to_silver_payment_vendor_dims`
3. **Fact Table**: `nb_bronze_to_silver_fact_trips` (requires all dimensions)
4. **Quality Validation**: Run validation notebook

### Important Dependencies Between Components
- Fact table notebook depends on ALL dimension notebooks being completed
- All silver layer notebooks require bronze layer data to exist
- Semantic model requires silver layer tables to be populated
- Reference data (`df_taxi_zones`) enriches location dimensions

## Development Environment

This is a Microsoft Fabric project - there are no traditional build/test commands. Development happens within the Fabric workspace:

- **No local execution**: All notebooks run in Fabric's Spark runtime
- **No package management**: Dependencies provided by Fabric environment
- **Git integration**: Use Fabric's Git integration to sync changes

## Common Tasks

### Running the Pipeline
```python
# No CLI commands - use Fabric UI or REST API
# Manual execution: Follow the sequence in "Data Processing Order" above
```

### Working with Notebooks
- Notebooks use PySpark with Fabric's runtime
- Default lakehouse must be set in each notebook
- Use `spark.sql()` for SQL queries and `df.write.format("delta")` for outputs

### Data Quality Checks
The notebooks include embedded quality checks:
- Null value validation
- Range checks (e.g., trip_duration < 8 hours)
- Referential integrity between fact and dimension tables
- Row count validations

## Technical Patterns

### Delta Lake Operations
```python
# Reading from bronze
df = spark.read.format("delta").load("Tables/bronze_trips")

# Writing to silver with overwrite
df.write.format("delta").mode("overwrite").save("Tables/silver_fact_trips")

# Incremental processing pattern
df.filter(col("date") > last_processed_date)
```

### Performance Optimizations
- Tables are partitioned by date: `partitionBy("trip_date")`
- Z-ordering on commonly filtered columns
- Cache dimensions: `dim_df.cache()`

### Error Handling
- Pipeline retry policies: `"retry": 1, "retryIntervalInSeconds": 30`
- Data validation filters: `.filter(col("value").isNotNull())`
- Outlier removal: `.filter(col("trip_duration") < 480)`

## Important Business Logic

### Time Dimension
- Includes holiday detection algorithm
- Fiscal quarters (October start)
- Time period classification (rush hours, etc.)

### Fact Table Calculations
- `tip_percentage = tip_amount / (total_amount - tip_amount) * 100`
- `cost_per_mile = total_amount / trip_distance`
- Foreign key generation using hash functions

### Data Quality Rules
- Trip distance > 0 and < 1000 miles
- Total amount > 0 and < 10000
- Passenger count between 1 and 8
- Valid datetime ranges

## Working with AI Features

The `nb_semantic_bronze_memory_analyzer` notebook uses Semantic Link for:
- Memory usage optimization
- Pattern detection in data
- Anomaly identification

## Troubleshooting

Common issues are documented in `PIPELINE_SETUP.md`:
- **Notebook reference errors**: Ensure all notebooks exist in workspace before creating pipeline
- **Lakehouse connection issues**: Set default lakehouse in notebook metadata
- **Data quality failures**: Check source data quality and filter conditions

## Key Files to Understand

1. **PIPELINE_SETUP.md**: Detailed setup instructions and execution flow
2. **Notebook files**: Contain the actual transformation logic
3. **pipeline-content.json**: Pipeline orchestration configuration
4. **semantic_bronze.SemanticModel/definition/**: TMDL model definition