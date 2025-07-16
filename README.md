# fabric-claude

A comprehensive data engineering solution exploring the integration of Claude AI with Microsoft Fabric for building intelligent data pipelines and semantic analysis workflows using NYC Taxi data.

## Overview

This repository demonstrates an end-to-end data engineering solution that combines AI-powered data processing using Claude with Microsoft Fabric's capabilities. It implements a medallion architecture (Bronze → Silver → Gold) for processing NYC Taxi trip data, featuring automated data pipelines, transformation notebooks, and semantic modeling.

## Project Structure

```
fabric-claude/
├── lh_claude_bronze.Lakehouse/              # Bronze layer data lakehouse
├── lh_silver.Lakehouse/                     # Silver layer data lakehouse
├── df_taxi_zones.Dataflow/                  # Taxi zones reference data ingestion
├── pipe_nyTaxi_to_bronze.DataPipeline/      # NYC Taxi data ingestion pipeline
├── nb_bronze_to_silver_fact_trips.Notebook/ # Fact table transformation
├── nb_bronze_to_silver_location_dim.Notebook/    # Location dimension processing
├── nb_bronze_to_silver_payment_vendor_dims.Notebook/ # Payment/vendor dimensions
├── nb_bronze_to_silver_time_dim.Notebook/   # Time dimension processing
├── nb_quality_validation.Notebook/          # Data quality validation
├── nb_semantic_bronze_memory analyzer.Notebook/  # Semantic analysis with AI
├── semantic_bronze.SemanticModel/           # Business intelligence model
└── PIPELINE_SETUP.md                        # Detailed setup instructions
```

## Architecture

### Medallion Architecture Implementation

```
┌─────────────────────┐
│   External Data     │
│  (NYC Taxi Data)    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌─────────────────────┐
│    Bronze Layer     │     │   Reference Data    │
│ (lh_claude_bronze)  │◄────┤  (df_taxi_zones)    │
└──────────┬──────────┘     └─────────────────────┘
           │
           ▼
┌─────────────────────┐
│ Transformation      │
│ Notebooks:          │
│ • Time Dimension    │
│ • Location Dim      │
│ • Payment/Vendor    │
│ • Fact Trips        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│    Silver Layer     │
│   (lh_silver)       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Quality Checks &   │
│ Semantic Analysis   │
└─────────────────────┘
```

## Components

### Data Lakehouses

#### Bronze Layer (`lh_claude_bronze`)
- **Purpose**: Raw data storage with minimal processing
- **Contents**: NYC taxi trip records in their original format
- **Features**: Data versioning, schema evolution support

#### Silver Layer (`lh_silver`)
- **Purpose**: Cleaned, validated, and transformed data
- **Contents**: Dimensional model with fact and dimension tables
- **Features**: Business-ready data, optimized for analytics

### Data Pipelines

#### NYC Taxi Data Ingestion (`pipe_nyTaxi_to_bronze`)
- **Source**: NYC Taxi & Limousine Commission datasets
- **Schedule**: Configurable (daily/weekly/monthly)
- **Process**: Downloads and ingests taxi trip data into bronze lakehouse

#### Taxi Zones Reference Data (`df_taxi_zones`)
- **Purpose**: Enriches trip data with location information
- **Source**: NYC TLC taxi zone lookup data
- **Output**: Reference table for location dimensions

### Transformation Notebooks

#### Dimension Processing (Parallel Execution)
1. **Time Dimension** (`nb_bronze_to_silver_time_dim`)
   - Creates date/time hierarchies
   - Generates calendar attributes
   - Supports time-based analytics

2. **Location Dimension** (`nb_bronze_to_silver_location_dim`)
   - Processes pickup/dropoff locations
   - Enriches with zone information
   - Creates geographic hierarchies

3. **Payment & Vendor Dimensions** (`nb_bronze_to_silver_payment_vendor_dims`)
   - Standardizes payment types
   - Creates vendor lookup tables
   - Handles categorical data

#### Fact Table Processing
**Fact Trips** (`nb_bronze_to_silver_fact_trips`)
- Aggregates trip metrics
- Links to all dimension tables
- Calculates derived measures
- Optimizes for query performance

### Quality & Analysis

#### Data Quality Validation (`nb_quality_validation`)
- Validates data completeness
- Checks referential integrity
- Generates quality reports
- Monitors data freshness

#### Semantic Analysis (`nb_semantic_bronze_memory_analyzer`)
- AI-powered pattern detection
- Memory usage optimization
- Semantic relationship discovery
- Anomaly detection

### Business Intelligence

**Semantic Model** (`semantic_bronze`)
- Tabular model for Power BI
- Pre-defined measures and KPIs
- Relationships and hierarchies
- Row-level security support

## Technologies Used

- **Microsoft Fabric**: Unified data platform for analytics
- **Claude AI**: Advanced language model for intelligent data processing
- **Python/PySpark**: Data transformation and processing
- **Delta Lake**: ACID-compliant data storage format
- **Power BI**: Business intelligence and visualization
- **NYC Taxi Data**: Real-world dataset for demonstrations

## Getting Started

### Prerequisites

- Microsoft Fabric workspace with appropriate permissions
- Python 3.8+ environment
- Access to Fabric Data Engineering experience
- (Optional) Claude API access for AI features

### Initial Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/accUlf/fabric-claude.git
   cd fabric-claude
   ```

2. **Import to Fabric Workspace**
   - Connect your Fabric workspace to this Git repository
   - Import all artifacts (lakehouses, notebooks, pipelines)
   - Follow the instructions in `PIPELINE_SETUP.md`

3. **Configure Lakehouses**
   - Create `lh_claude_bronze` and `lh_silver` lakehouses
   - Set default lakehouses in each notebook
   - Configure data source connections

### Data Pipeline Execution

#### Option 1: Automated Pipeline
After importing all notebooks to your workspace:
1. Create the orchestration pipeline
2. Configure pipeline parameters
3. Schedule or trigger manual execution

#### Option 2: Manual Execution
Run notebooks in this order:
1. **Parallel**: Time, Location, and Payment/Vendor dimensions
2. **Sequential**: Fact Trips transformation
3. **Final**: Quality validation

## Use Cases

### 1. **Real-time Analytics Platform**
- Process streaming taxi data
- Generate live dashboards
- Monitor city transportation patterns

### 2. **AI-Enhanced Data Quality**
- Automated anomaly detection
- Intelligent data profiling
- Pattern recognition in trip data

### 3. **Predictive Analytics**
- Demand forecasting
- Route optimization
- Pricing analysis

### 4. **Learning Platform**
- Understand medallion architecture
- Practice data engineering concepts
- Explore AI/ML integration patterns

## Key Features

- **Incremental Data Processing**: Efficient handling of large datasets
- **Schema Evolution**: Automatic handling of source data changes
- **Data Lineage**: Full traceability from source to consumption
- **Performance Optimization**: Partitioning and indexing strategies
- **Error Handling**: Robust failure recovery mechanisms

## Performance Considerations

- **Partitioning**: Data is partitioned by date for optimal query performance
- **Caching**: Frequently accessed dimensions are cached
- **Compression**: Delta Lake provides efficient compression
- **Indexing**: Z-ordering on commonly filtered columns

## Monitoring & Operations

### Data Quality Metrics
- Row count validations
- Null value checks
- Referential integrity
- Data freshness indicators

### Pipeline Monitoring
- Execution time tracking
- Resource utilization
- Error logs and alerts
- Success/failure notifications

## Troubleshooting

See `PIPELINE_SETUP.md` for detailed troubleshooting steps including:
- Notebook reference errors
- Pipeline dependency issues
- Data quality failures
- Performance optimization tips

## Future Enhancements

- [ ] Gold layer implementation
- [ ] Real-time streaming pipeline
- [ ] Advanced AI/ML models
- [ ] Automated data cataloging
- [ ] Enhanced security features
- [ ] Multi-region support

## Contributing

This is an experimental project for exploring AI and data engineering concepts. Contributions are welcome:

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Submit a pull request

## Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- [NYC Taxi Dataset Information](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark Best Practices](https://spark.apache.org/docs/latest/api/python/)

## License

This project is for educational and experimental purposes. Please check with the repository owner for specific licensing information.

## Contact

Repository maintained by: [accUlf](https://github.com/accUlf)

---

*This project demonstrates the power of combining AI capabilities with modern data engineering practices in Microsoft Fabric.*