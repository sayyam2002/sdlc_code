# AWS Glue ETL Pipeline - Customer Order Processing with SCD Type 2

## Overview
This AWS Glue job implements a complete ETL pipeline for processing customer and order data with the following capabilities:
- Ingestion of customer and order data from S3 CSV files
- Data cleaning (null removal and deduplication)
- AWS Glue Data Catalog registration
- SCD Type 2 implementation using Apache Hudi for order summary
- Customer aggregate spend calculation

## Architecture
- **Source Data**: S3 CSV files (customer and order data)
- **Processing**: AWS Glue with PySpark and Apache Hudi
- **Storage**: S3 (curated and analytics layers)
- **Catalog**: AWS Glue Data Catalog
- **Query**: Amazon Athena
- **Monitoring**: Amazon CloudWatch

## Prerequisites
- AWS Glue 3.0 or later (for Hudi support)
- Python 3.x
- IAM role with permissions for S3, Glue Data Catalog, CloudWatch, and Athena
- Glue database `gen_ai_poc_databrickscoe` must exist

## Configuration
All runtime parameters are defined in `config/glue_params.yaml` with sensible defaults:
- Source paths for customer and order data
- Target paths for curated and analytics data
- Catalog database and table names
- Hudi configuration parameters

## Project Structure
```
.
├── README.md
├── requirements.txt
├── config/
│   └── glue_params.yaml
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   └── job.py
│   └── test/
│       ├── __init__.py
│       └── test_job.py
└── sample_data/
    └── input.csv
```

## Deployment
1. Upload `src/main/job.py` to S3
2. Create AWS Glue job pointing to the script location
3. Configure job parameters from `config/glue_params.yaml`
4. Set Glue version to 3.0 or later
5. Assign appropriate IAM role

## Execution Flow
1. **Ingestion**: Read customer and order CSV data from S3
2. **Cleaning**: Remove null values and duplicates
3. **Catalog Registration**: Register cleaned data in Glue Data Catalog
4. **SCD2 Processing**: Join customer and order data, apply SCD Type 2 logic using Hudi
5. **Aggregation**: Calculate customer aggregate spend
6. **Final Catalog Registration**: Register ordersummary and customeraggregatespend tables

## Testing
Run unit tests locally:
```bash
python -m pytest src/test/test_job.py -v
```

Tests use mocked Spark and Glue contexts to validate transformation logic without AWS dependencies.

## Monitoring
- CloudWatch Logs: Job execution logs with record counts and processing steps
- CloudWatch Metrics: Custom metrics for records ingested, cleaned, and processed
- Athena: Query cataloged tables for data validation

## Data Quality
- Null value removal: Filters out records with "Null" string or NULL values
- Deduplication: Removes duplicate records based on all fields
- Type casting: Ensures numeric fields are properly typed
- SCD2 validation: Maintains historical records with proper metadata

## Assumptions
- SCD2 record key: CustId + OrderId (composite key)
- Hudi table type: COPY_ON_WRITE
- Aggregation level: Daily (grouped by Name and Date)
- File format for analytics: Parquet
- Update mode: Overwrite for catalog tables, UPSERT for Hudi

## Support
For issues or questions, refer to the Technical Requirements Document (TRD) for detailed specifications.