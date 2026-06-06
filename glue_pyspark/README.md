# AWS Glue PySpark Job - Customer and Order Data Processing

## Overview
This AWS Glue PySpark job processes customer and order data from S3, performs data cleaning, applies transformations, and writes curated data to S3 with Glue Data Catalog integration.

## Features
- Ingests customer and order CSV data from S3
- Performs data quality operations (null removal, deduplication)
- Calculates customer aggregate spending
- Writes curated data in Parquet format
- Registers tables in AWS Glue Data Catalog
- Implements Apache Hudi for SCD Type 2 tracking
- Comprehensive error handling and logging

## Project Structure
```
glue_pyspark/
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
    ├── customer_data.csv
    └── order_data.csv
```

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- Source S3 paths for customer and order data
- Target S3 paths for curated data
- Glue Data Catalog database and table names
- Hudi configuration for SCD Type 2

## Running the Job

### AWS Glue Console
1. Upload the job script to S3
2. Create a new Glue job pointing to the script
3. Configure job parameters from `glue_params.yaml`
4. Run the job

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest src/test/test_job.py -v
```

## Data Flow
1. **Ingest**: Read customer and order CSV files from S3
2. **Clean**: Remove nulls and duplicates
3. **Transform**: Calculate aggregations and apply business logic
4. **Curate**: Write cleaned data to S3 in Parquet format
5. **Catalog**: Register tables in Glue Data Catalog
6. **Analytics**: Generate customer aggregate spending reports

## Schema Definitions

### Customer Schema
- CustId (String, Not Null)
- Name (String, Not Null)
- EmailId (String, Not Null)
- Region (String, Not Null)

### Order Schema
- OrderId (String, Not Null)
- ItemName (String, Not Null)
- PricePerUnit (Decimal(10,2), Not Null)
- Qty (Integer, Not Null)
- Date (String, Not Null)
- CustId (String, Not Null)

### SCD Type 2 Columns
- IsActive (Boolean)
- StartDate (Timestamp)
- EndDate (Timestamp)
- OpTs (Timestamp)

## Dependencies
- PySpark 3.x
- AWS Glue libraries
- Apache Hudi (included in Glue)

## Error Handling
- S3 access validation before operations
- Schema validation on read
- Comprehensive logging throughout pipeline
- Graceful failure handling with informative messages