# Pipeline Setup Instructions

## Problem
The orchestration pipeline `pipe_bronze_to_silver_orchestration` cannot reference notebooks that don't exist in the Fabric workspace yet. This causes the error:
> "The document creation or update failed because of invalid reference 'nb_bronze_to_silver_time_dim'"

## Solution
Follow these steps to set up the complete pipeline:

### Step 1: Create All Notebooks First
Before creating the pipeline, ensure all notebooks are created in your Fabric workspace:

1. **nb_bronze_to_silver_time_dim** - Time dimension transformation
2. **nb_bronze_to_silver_location_dim** - Location dimension transformation  
3. **nb_bronze_to_silver_payment_vendor_dims** - Payment/vendor/rate/trip type dimensions
4. **nb_bronze_to_silver_fact_trips** - Fact table transformation
5. **nb_silver_quality_checks** - Data quality validation

### Step 2: Update Pipeline Configuration
Once all notebooks exist, update the pipeline configuration:

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
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "notebook": {
            "referenceName": "nb_bronze_to_silver_time_dim",
            "type": "NotebookReference"
          }
        }
      },
      {
        "name": "Transform Location Dimension",
        "type": "SynapseNotebook",
        "dependsOn": [],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 1,
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "notebook": {
            "referenceName": "nb_bronze_to_silver_location_dim",
            "type": "NotebookReference"
          }
        }
      },
      {
        "name": "Transform Reference Dimensions",
        "type": "SynapseNotebook",
        "dependsOn": [],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 1,
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "notebook": {
            "referenceName": "nb_bronze_to_silver_payment_vendor_dims",
            "type": "NotebookReference"
          }
        }
      },
      {
        "name": "Transform Fact Table",
        "type": "SynapseNotebook",
        "dependsOn": [
          {"activity": "Transform Time Dimension", "dependencyConditions": ["Succeeded"]},
          {"activity": "Transform Location Dimension", "dependencyConditions": ["Succeeded"]},
          {"activity": "Transform Reference Dimensions", "dependencyConditions": ["Succeeded"]}
        ],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 1,
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "notebook": {
            "referenceName": "nb_bronze_to_silver_fact_trips",
            "type": "NotebookReference"
          }
        }
      },
      {
        "name": "Data Quality Validation",
        "type": "SynapseNotebook",
        "dependsOn": [
          {"activity": "Transform Fact Table", "dependencyConditions": ["Succeeded"]}
        ],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 1,
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "notebook": {
            "referenceName": "nb_silver_quality_checks",
            "type": "NotebookReference"
          }
        }
      }
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

### Step 3: Execution Order
The pipeline executes in this order:
1. **Parallel**: Time, Location, and Reference dimensions
2. **Sequential**: Fact table (waits for all dimensions)
3. **Final**: Data quality validation

### Step 4: Alternative Approach - Manual Execution
If the automated pipeline continues to have issues, you can run the notebooks manually in this order:

1. Run these **in parallel** (any order):
   - `nb_bronze_to_silver_time_dim`
   - `nb_bronze_to_silver_location_dim` 
   - `nb_bronze_to_silver_payment_vendor_dims`

2. After all dimension notebooks complete, run:
   - `nb_bronze_to_silver_fact_trips`

3. Finally, run:
   - `nb_silver_quality_checks`

## Troubleshooting
- **Invalid Reference Error**: Ensure all notebooks exist in the workspace before creating the pipeline
- **Timeout Issues**: Increase timeout values in the policy section
- **Dependency Failures**: Check that dimension notebooks completed successfully before fact table runs
- **Data Quality Failures**: Review the quality check results and fix any data issues

## Next Steps
1. Create all notebooks in your Fabric workspace
2. Update the pipeline configuration with the complete JSON above
3. Test the pipeline execution
4. Set up scheduling if needed