# AWS Glue PySpark Job - SDLC Wizard Data Pipeline

## Overview
This AWS Glue PySpark job processes customer and order data with SCD Type 2 implementation using Apache Hudi.

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Paths
- **Curated Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`
- **Catalog**: `s3://adif-sdlc/catalog/sdlc_wizard_order/`

## Features
- SCD Type 2 implementation with Hudi
- Data cleaning (NULL removal, duplicate removal)
- Customer-Order aggregation
- Parquet output format

## Schemas

### Customer Schema
- CustId (string)
- Name (string)
- EmailId (string)
- Region (string)

### Order Schema
- OrderId (string)
- ItemName (string)
- PricePerUnit (double)
- Qty (integer)
- Date (string)
- CustId (string)

### SCD Type 2 Columns
- IsActive (boolean)
- StartDate (timestamp)
- EndDate (timestamp)
- OpTs (timestamp)

## Job Parameters
- `--customer_source_path`: S3 path for customer data
- `--order_source_path`: S3 path for order data
- `--curated_output_path`: S3 path for curated order summary
- `--analytics_output_path`: S3 path for customer aggregate spend
- `--catalog_output_path`: S3 path for catalog data

## Running the Job
```bash
aws glue start-job-run --job-name sdlc-wizard-pipeline \
  --arguments='--customer_source_path=s3://adif-sdlc/sdlc_wizard/customerdata/,--order_source_path=s3://adif-sdlc/sdlc_wizard/orderdata/,--curated_output_path=s3://adif-sdlc/curated/sdlc_wizard/ordersummary/,--analytics_output_path=s3://adif-sdlc/analytics/customeraggregatespend/,--catalog_output_path=s3://adif-sdlc/catalog/sdlc_wizard_order/'
```

## Testing
```bash
pytest tests/test_job.py -v
```

## Requirements
- AWS Glue 4.0+
- Apache Hudi
- PySpark 3.3+