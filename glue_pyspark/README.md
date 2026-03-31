# AWS Glue PySpark - Customer Order Data Processing with SCD Type 2

## Overview
This AWS Glue job processes customer and order data from S3, applies data cleaning, performs aggregations, and implements SCD Type 2 using Apache Hudi format.

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Paths
- **Curated Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`
- **Curated Customer Data (SCD2)**: `s3://adif-sdlc/curated/sdlc_wizard/`

## Schema

### Customer Data
- `CustId` (string) - Primary Key
- `Name` (string)
- `EmailId` (string)
- `Region` (string)

### Order Data
- `OrderId` (string) - Primary Key
- `CustId` (string) - Foreign Key
- `OrderDate` (string)
- `Amount` (double)

### SCD Type 2 Columns (Added)
- `IsActive` (boolean)
- `StartDate` (timestamp)
- `EndDate` (timestamp)
- `OpTs` (timestamp)

## Features
1. **Data Ingestion**: Read CSV files from S3 with headers
2. **Data Cleaning**: Remove NULL values, 'Null' strings, and duplicates
3. **SCD Type 2**: Track historical changes using Hudi format
4. **Aggregations**: Calculate customer aggregate spend
5. **Order Summary**: Generate curated order summary with customer details

## Requirements
- AWS Glue 3.0+
- Python 3.7+
- Apache Hudi support
- S3 permissions: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`

## Job Parameters
- `--JOB_NAME`: Glue job name
- `--customer_source_path`: S3 path for customer data (default: s3://adif-sdlc/sdlc_wizard/customerdata/)
- `--order_source_path`: S3 path for order data (default: s3://adif-sdlc/sdlc_wizard/orderdata/)
- `--curated_customer_path`: S3 path for curated customer data (default: s3://adif-sdlc/curated/sdlc_wizard/)
- `--order_summary_path`: S3 path for order summary (default: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/)
- `--aggregate_spend_path`: S3 path for aggregate spend (default: s3://adif-sdlc/analytics/customeraggregatespend/)

## Running the Job
```bash
python src/main/job.py \
  --JOB_NAME customer_order_processing \
  --customer_source_path s3://adif-sdlc/sdlc_wizard/customerdata/ \
  --order_source_path s3://adif-sdlc/sdlc_wizard/orderdata/
```

## Testing
```bash
pip install -r requirements.txt
pytest src/test/test_job.py -v
```

## Architecture
1. Read customer and order CSV files from S3
2. Clean data (remove NULLs, 'Null' strings, duplicates)
3. Apply SCD Type 2 logic to customer data
4. Write customer data to Hudi format
5. Generate order summary with customer details
6. Calculate customer aggregate spend
7. Write outputs to S3 in Parquet format