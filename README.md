# fabric-claude

A testing environment exploring the integration of Claude AI with Microsoft Fabric for data engineering and semantic analysis workflows.

## Overview

This repository demonstrates the combination of AI-powered data processing using Claude and Microsoft Fabric's data engineering capabilities. It serves as an experimental playground for building intelligent data pipelines and semantic analysis workflows.

## Project Structure

```
fabric-claude/
├── lh_claude_bronze.Lakehouse/       # Bronze layer data lakehouse
├── nb_semantic_bronze_memory_analyzer.Notebook/  # Semantic analysis notebook
├── pipe_nyTaxi_to_bronze.DataPipeline/  # NYC Taxi data ingestion pipeline
└── semantic_bronze.SemanticModel/    # Semantic model definitions
```

## Components

### Data Lakehouse (Bronze Layer)
- **Location**: `lh_claude_bronze.Lakehouse/`
- **Purpose**: Storage layer for raw and minimally processed data
- **Description**: Implements the bronze layer of a medallion architecture for data organization

### Semantic Analysis Notebook
- **Location**: `nb_semantic_bronze_memory_analyzer.Notebook/`
- **Purpose**: Analyzes semantic patterns and memory usage
- **Features**: Interactive exploration of data semantics using AI-powered analysis

### NYC Taxi Data Pipeline
- **Location**: `pipe_nyTaxi_to_bronze.DataPipeline/`
- **Purpose**: Ingests NYC taxi trip data into the bronze layer
- **Data Flow**: Raw data → Processing → Bronze lakehouse storage

### Semantic Model
- **Location**: `semantic_bronze.SemanticModel/`
- **Purpose**: Defines semantic relationships and business logic
- **Usage**: Powers analytical queries and AI-enhanced insights

## Technologies Used

- **Microsoft Fabric**: Data engineering platform
- **Claude AI**: AI-powered analysis and processing
- **Python**: Primary programming language
- **Data Lakehouse**: Modern data architecture pattern

## Getting Started

1. **Prerequisites**
   - Microsoft Fabric workspace access
   - Python environment
   - API access for Claude (if applicable)

2. **Setup**
   ```bash
   git clone https://github.com/accUlf/fabric-claude.git
   cd fabric-claude
   ```

3. **Configuration**
   - Configure your Fabric workspace connection
   - Set up necessary API credentials
   - Review pipeline configurations

## Use Cases

- **Data Engineering**: Build intelligent data pipelines with AI assistance
- **Semantic Analysis**: Extract meaning and patterns from structured data
- **Experimentation**: Test integration patterns between AI and data platforms
- **Learning**: Explore modern data architecture with AI capabilities

## Architecture

This project implements a medallion architecture pattern:
- **Bronze Layer**: Raw data storage (implemented)
- **Silver Layer**: Cleaned and validated data (future enhancement)
- **Gold Layer**: Business-ready datasets (future enhancement)

## Contributing

This is a testing environment for exploring AI and data engineering concepts. Feel free to fork and experiment with your own variations.

## License

Please check with the repository owner for licensing information.

## Contact

Repository maintained by: [accUlf](https://github.com/accUlf)