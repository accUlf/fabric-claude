# Pipeline Setup Instructions

## Problem
The orchestration pipeline `pipe_bronze_to_silver_orchestration` cannot reference notebooks that don't exist in the Fabric workspace yet. This causes the error:
> "The document creation or update failed because of invalid reference 'nb_bronze_to_silver_time_dim'"

## Root Cause
- **Fabric pipelines can only reference notebooks that already exist in the workspace**
- **The notebooks exist in git but haven't been created in your Fabric workspace yet**
- **Chicken-and-egg problem**: Pipeline needs notebooks to exist, but notebooks need to be created first

## Solution Options

### Option 1: Create Notebooks First, Then Pipeline
1. **Create all notebooks in your Fabric workspace first**:
   - `nb_bronze_to_silver_time_dim`
   - `nb_bronze_to_silver_location_dim`
   - `nb_bronze_to_silver_payment_vendor_dims`
   - `nb_bronze_to_silver_fact_trips`
   - `nb_quality_validation`

2. **Then create the pipeline** - it will be able to reference the existing notebooks

### Option 2: Manual Execution (Recommended if pipeline fails)
Run the notebooks manually in this specific order:

#### Phase 1: Dimension Processing (Run in Parallel)
Execute these notebooks simultaneously - they don't depend on each other:
- `nb_bronze_to_silver_time_dim`
- `nb_bronze_to_silver_location_dim`
- `nb_bronze_to_silver_payment_vendor_dims`

#### Phase 2: Fact Table Processing (Sequential)
After ALL dimension notebooks complete successfully:
- `nb_bronze_to_silver_fact_trips`

#### Phase 3: Quality Validation (Final)
After the fact table completes:
- `nb_quality_validation`

## Expected Pipeline Configuration
Once all notebooks exist, the pipeline should work with this configuration:

```json
{
  "properties": {
    "activities": [
      {
        "name": "Transform Time Dimension",
        "type": "SynapseNotebook",
        "dependsOn": [],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 1,
          "retryIntervalInSeconds": 30,
          "secureOutput": false,
          "secureInput": false
        },
        "typeProperties": {
          "notebook": {
            "referenceName": "nb_bronze_to_silver_time_dim",
            "type": "NotebookReference"
          },
          "parameters": {}
        }
      }
      // ... other activities
    ],
    "parameters": {
      "run_date": {
        "type": "string",
        "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
      }
    }
  }
}
```

## Data Flow Architecture
```
Bronze Layer (lh_claude_bronze)
    │
    ├── nb_bronze_to_silver_time_dim ────────┐
    ├── nb_bronze_to_silver_location_dim ────┤
    ├── nb_bronze_to_silver_payment_vendor_dims ──┤
    │                                         │
    │                                         ▼
    └── nb_bronze_to_silver_fact_trips ──► Silver Layer (lh_silver)
                                            │
                                            ▼
                                    nb_quality_validation
```

## Troubleshooting

### Error: "Invalid reference 'nb_bronze_to_silver_time_dim'"
- **Solution**: Create the notebook in your workspace first
- **Check**: Verify notebook exists in Fabric workspace (not just git)

### Error: "Reserved name conflict"
- **Solution**: Already fixed - notebook renamed to `nb_quality_validation`
- **Check**: Ensure you're using the new notebook name

### Pipeline Dependency Failures
- **Solution**: Ensure dimension notebooks complete before fact table runs
- **Check**: Review execution logs for specific error messages

### Data Quality Validation Failures
- **Solution**: Check quality report output in the validation notebook
- **Check**: Verify source data quality in bronze layer

## Production Deployment
1. **Test Environment**: Run notebooks manually first to verify data flow
2. **Staging**: Create pipeline with all notebooks existing
3. **Production**: Schedule pipeline for daily execution (suggested: 2 AM)
4. **Monitoring**: Set up alerts for pipeline failures

## Alternative: Step-by-Step Manual Process
If you prefer to avoid the pipeline complexity initially:

1. **Run Dimensions**:
   ```
   Execute simultaneously:
   - nb_bronze_to_silver_time_dim
   - nb_bronze_to_silver_location_dim
   - nb_bronze_to_silver_payment_vendor_dims
   ```

2. **Wait for Completion**, then run **Fact Table**:
   ```
   Execute after all dimensions complete:
   - nb_bronze_to_silver_fact_trips
   ```

3. **Run Quality Checks**:
   ```
   Execute after fact table completes:
   - nb_quality_validation
   ```

This approach gives you full control and visibility into each step of the transformation process.