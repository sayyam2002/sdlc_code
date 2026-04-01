# AWS Glue PySpark Job - Customer Order Processing

## Overview
This AWS Glue PySpark job processes customer and order data with the following capabilities:
- Ingests customer and order CSV files from S3
- Cleans data by removing nulls and duplicates
- Registers cleaned data in AWS Glue Data Catalog
- Implements SCD Type 2 for order summary using Apache Hudi
- Performs incremental processing based on customer changes
- Calculates customer aggregate spending

## Architecture
- **Source Data**: CSV files in S3
- **Processing**: AWS Glue PySpark with DataFrame API
- **Storage**: Apache Hudi (Copy-on-Write) for SCD Type 2
- **Catalog**: AWS Glue Data Catalog for Athena querying
- **Output**: Parquet and Hudi formats

## Job Parameters
The job accepts the following runtime arguments:

- `--customer_source_path`: S3 path for customer CSV files (default: s3://adif-sdlc/sdlc_wizard/customerdata/)
- `--order_source_path`: S3 path for order CSV files (default: s3://adif-sdlc/sdlc_wizard/orderdata/)
- `--customer_output_path`: S3 path for cleaned customer data (default: s3://adif-sdlc/curated/sdlc_wizard/customer/)
- `--order_output_path`: S3 path for cleaned order data (default: s3://adif-sdlc/curated/sdlc_wizard/order/)
- `--ordersummary_output_path`: S3 path for order summary Hudi table (default: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/)
- `--customer_snapshot_path`: S3 path for customer snapshot (default: s3://adif-sdlc/curated/sdlc_wizard/customer_snapshot/)
- `--aggregate_output_path`: S3 path for customer aggregate spend (default: s3://adif-sdlc/analytics/customeraggregatespend/)
- `--database_name`: Glue catalog database name (required)
- `--customer_table_name`: Customer table name (default: customer)
- `--order_table_name`: Order table name (default: order)
- `--ordersummary_table_name`: Order summary table name (default: ordersummary)
- `--aggregate_table_name`: Aggregate table name (default: customeraggregatespend)

## Execution
```bash
aws glue start-job-run \
  --job-name <job-name> \
  --arguments='--database_name=<database>,--customer_source_path=s3://...'
```

## Testing
Run unit tests:
```bash
python -m pytest src/test/test_job.py -v
```

## Data Flow
1. **Ingest**: Read customer and order CSV files from S3
2. **Clean**: Remove nulls and duplicates
3. **Catalog**: Write cleaned data to S3 and register in Glue Catalog
4. **Detect Changes**: Compare current customers with previous snapshot
5. **SCD Type 2**: Join and upsert order summary with historical tracking
6. **Aggregate**: Calculate total spending per customer
7. **Snapshot**: Save current customer state for next run

## Schema Definitions

### Customer Schema
- CustId (String)
- Name (String)
- EmailId (String)
- Region (String)

### Order Schema
- OrderId (String)
- ItemName (String)
- PricePerUnit (Double)
- Qty (Long)
- Date (String, YYYY-MM-DD)
- CustId (String)

### Order Summary Schema (SCD Type 2)
- CustId (String)
- OrderId (String)
- Name (String)
- EmailId (String)
- Region (String)
- ItemName (String)
- PricePerUnit (Double)
- Qty (Long)
- Date (String)
- TotalAmount (Double)
- IsActive (Boolean)
- StartDate (Timestamp)
- EndDate (Timestamp)
- OpTs (Timestamp)