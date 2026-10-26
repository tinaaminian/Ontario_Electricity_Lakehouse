# Ontario Electricity Demand Lakehouse

An end-to-end batch data engineering pipeline built with Databricks, PySpark, Delta Lake, and Unity Catalog to process Ontario's hourly electricity demand data published by the Independent Electricity System Operator (IESO).

The pipeline follows the Medallion Architecture (Bronze, Silver, Gold), with incremental file ingestion, data transformation, quality validation, daily aggregations, and orchestration through Databricks Lakeflow Jobs.

## Architecture

```text
IESO Hourly Electricity Demand CSV (2025)
                  |
                  v
        Unity Catalog Volume
                  |
                  v
        Bronze - Auto Loader
                  |
                  v
     Silver - Transform & Validate
                  |
                  v
      Gold - Daily Demand Summary
```

The three processing stages are orchestrated using Databricks Lakeflow Jobs with explicit task dependencies.

## Tech Stack

- **Databricks** — Data processing and workflow orchestration
- **PySpark** — DataFrame transformations and aggregations
- **Delta Lake** — Storage format for Bronze, Silver, and Gold tables
- **Unity Catalog** — Data organization and governance
- **Auto Loader** — Incremental CSV ingestion with checkpointing
- **Lakeflow Jobs** — Workflow orchestration, task dependencies, and execution monitoring
- **Python / GitHub** — Modular pipeline development and version control

## Dataset

Source: IESO Ontario Hourly Electricity Demand, 2025.

The dataset contains:

- Date
- Hour ending (1–24)
- Market Demand (MW)
- Ontario Demand (MW)

**Total source records:** 8,759

During data profiling, I identified a missing hourly observation for May 1, 2025 (hour ending 1). The observation is absent from the original source file, so no artificial record was generated.

## Implementation

### 1. Bronze Layer — Incremental Ingestion

Raw CSV files are stored in a Unity Catalog Volume and ingested into a Bronze Delta table using Databricks Auto Loader.

Key implementations:

- Defined the source schema explicitly.
- Implemented incremental file ingestion using Auto Loader.
- Used checkpointing to track processed files.
- Configured `availableNow` for batch-style stream execution.
- Preserved raw source values before transformation.
- Added ingestion metadata:
  - Source file path
  - Source file name
  - Ingestion timestamp

**Output table:**
`ontario_energy.bronze.ieso_demand_2025`

### 2. Silver Layer — Transformation and Validation

The Silver layer converts raw source data into typed, validated records.

Key implementations:

- Converted source dates and hours into appropriate data types.
- Used `try_cast` for safe numeric conversions.
- Used decimal types for electricity demand values.
- Preserved source metadata for traceability.
- Validated required fields and numeric values.
- Checked that hour-ending values fall between 1 and 24.
- Checked that electricity demand values are positive.
- Detected duplicate records using the composite key `(demand_date, hour_ending)`.
- Reconciled row counts between Bronze and Silver.
- Implemented full-refresh Delta writes for the fixed annual dataset.


**Output table:**
`ontario_energy.silver.ieso_demand_2025`

### 3. Gold Layer — Daily Demand Aggregation

The Gold layer produces daily electricity demand metrics from the validated hourly records.

**Grain:** One row per calendar date.

Key implementations:

- Aggregated hourly records using PySpark `groupBy()` and aggregate functions.
- Calculated average, peak, and minimum electricity demand.
- Counted hourly observations per day.
- Used full-refresh Delta writes.
- Validated the persisted Gold table.

The Gold dataset contains **365 daily records**.

May 1, 2025 contains 23 hourly observations and is marked as incomplete.

**Output table:**
`ontario_energy.gold.daily_demand_summary`

### 4. Workflow Orchestration — Databricks Lakeflow Jobs

Created a three-task Databricks Job:

```text
bronze_ingestion
       |
       v
silver_transformation
       |
       v
gold_daily_summary
```

Each task executes a dedicated notebook that imports reusable Python functions from the `src/` directory.

Implemented:

- Explicit dependencies between tasks.
- Sequential Bronze → Silver → Gold execution.
- Validation before writing transformed data.
- Exceptions to fail tasks when validation fails.
- Serverless compute for notebook execution.

The complete workflow was executed successfully in Databricks.



## Design Decisions

**Incremental Bronze ingestion:** Auto Loader and checkpointing track previously processed files, allowing the ingestion stage to be rerun without reloading the same source files under the same checkpoint configuration.

**Raw data preservation:** Bronze retains the original source values, allowing transformations to be revised independently.

**Modular pipeline:** Ingestion, transformation, validation, and writing are implemented as separate Python functions and modules.

**Full-refresh Silver and Gold:** Since the pipeline processes a fixed 2025 dataset, overwrite operations are used instead of incremental merges.

**Data completeness:** Missing source observations are preserved as missing and reflected in the daily completeness indicator.

**Workflow dependencies:** Downstream tasks run only after their upstream tasks succeed, preventing invalid data from progressing through the pipeline.
