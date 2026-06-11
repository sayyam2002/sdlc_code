# AWS Glue ETL Pipeline - Use Case 1

## Overview
This AWS Glue ETL pipeline implements a complete data processing solution for customer and order data, including:
- Data ingestion from S3 (CSV format)
- Data cleaning (null removal and deduplication)
- AWS Glue Catalog registration
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend calculation

## Architecture
- **Source Data**: S3 CSV files (customer and order data)
- **Processing Engine**: AWS Glue with PySpark
- **Storage Format**: Apache Hudi (for SCD Type 2), Parquet (for catalog tables)
- **Catalog**: AWS Glue Data Catalog
- **Query Engine**: Amazon Athena

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

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- Source S3 paths for customer and order data
- Target S3 paths for curated and analytics layers
- Glue catalog database and table names
- Hudi configuration parameters

## Running the Job

### AWS Glue Console
1. Upload `src/main/job.py` to S3
2. Create a new Glue job pointing to the script
3. Configure job parameters from `config/glue_params.yaml`
4. Run the job

### Local Testing
```bash
pip install -r requirements.txt
python -m pytest src/test/test_job.py -v
```

## Data Flow
1. **Ingestion**: Read customer and order CSV files from S3
2. **Cleaning**: Remove null values and duplicates
3. **Catalog Registration**: Register cleaned data in Glue Catalog
4. **SCD Type 2**: Implement slowly changing dimension logic using Hudi
5. **Aggregation**: Calculate customer aggregate spend
6. **Catalog Update**: Register all output tables in Glue Catalog

## Key Features
- **Null Handling**: Removes both string "Null" and NULL values
- **Deduplication**: Removes duplicate records based on all columns
- **SCD Type 2**: Maintains historical records with IsActive, StartDate, EndDate, OpTs
- **Incremental Processing**: Supports incremental updates using Hudi upsert
- **Error Handling**: Comprehensive validation and error logging

## Requirements
- AWS Glue 4.0 or higher
- Python 3.9+
- Apache Hudi support (included in Glue 4.0)
- IAM permissions for S3, Glue Catalog, and CloudWatch

## Monitoring
- CloudWatch Logs: All job execution logs
- Glue Job Metrics: Duration, success/failure status
- Data Quality Metrics: Record counts before/after cleaning

## Notes
- Default configuration uses sample S3 paths from FRD
- Hudi table type: Copy-on-Write (COW)
- All timestamps use UTC timezone
- Deduplication retains first occurrence