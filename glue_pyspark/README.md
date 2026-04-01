# AWS Glue PySpark Job - SDLC Wizard Customer Order Analytics

## Overview
This AWS Glue PySpark job processes customer and order data with SCD Type 2 implementation using Apache Hudi format.

## Features
- **Data Ingestion**: Reads customer and order data from S3
- **Data Cleaning**: Removes NULL values, 'Null' strings, and duplicates
- **Data Transformation**: Joins customer and order data
- **SCD Type 2**: Tracks historical changes using Hudi format with IsActive, StartDate, EndDate, OpTs columns
- **Aggregation**: Calculates customer aggregate spend
- **Glue Catalog Integration**: Registers tables in AWS Glue Data Catalog

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Paths
- **Order Summary (SCD Type 2)**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schemas

### Customer Data
- CustId: string
- Name: string
- EmailId: string
- Region: string

### Order Data
- OrderId: string
- CustId: string
- OrderDate: string
- Amount: double
- ProductId: string

### Order Summary (SCD Type 2)
All columns from customer-order join plus:
- IsActive: boolean
- StartDate: timestamp
- EndDate: timestamp
- OpTs: timestamp

## Requirements
- Python 3.7+
- PySpark 3.x
- AWS Glue 3.0+
- Apache Hudi

## Installation
```bash
pip install -r requirements.txt
```

## Running Tests
```bash
pytest glue_pyspark/src/test/test_job.py -v
```

## AWS Glue Job Parameters
- `--JOB_NAME`: Name of the Glue job
- `--customer_source_path`: S3 path for customer data (default: s3://adif-sdlc/sdlc_wizard/customerdata/)
- `--order_source_path`: S3 path for order data (default: s3://adif-sdlc/sdlc_wizard/orderdata/)
- `--ordersummary_output_path`: S3 path for order summary output (default: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/)
- `--aggregate_output_path`: S3 path for aggregate output (default: s3://adif-sdlc/analytics/customeraggregatespend/)
- `--glue_database`: Glue catalog database name (default: sdlc_wizard_db)

## Architecture
1. **Ingestion**: Read customer and order data from S3
2. **Cleaning**: Remove NULL/Null values and duplicates
3. **Transformation**: Join customer and order data
4. **SCD Type 2**: Apply Hudi upsert with historical tracking
5. **Aggregation**: Calculate total spend per customer
6. **Catalog**: Register tables in Glue Data Catalog