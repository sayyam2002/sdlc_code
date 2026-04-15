# AWS Glue PySpark Job - Customer Order Analytics with SCD Type 2

## Overview
This AWS Glue job implements a complete ETL pipeline for customer and order data processing with SCD Type 2 historical tracking using Apache Hudi.

## Features
- **Data Ingestion**: Reads customer and order data from S3
- **Data Cleaning**: Removes NULL values, 'Null' strings, and duplicates
- **SCD Type 2**: Tracks historical changes using Hudi format
- **Data Aggregation**: Calculates customer aggregate spend
- **AWS Glue Catalog**: Registers tables for querying

## Data Sources (S3 Paths)
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Paths
- **Curated Customer**: `s3://adif-sdlc/curated/sdlc_wizard/customer/`
- **Curated Order**: `s3://adif-sdlc/curated/sdlc_wizard/order/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schemas

### Customer Schema
- CustId (string) - Primary Key
- Name (string)
- EmailId (string)
- Region (string)
- IsActive (boolean) - SCD Type 2
- StartDate (timestamp) - SCD Type 2
- EndDate (timestamp) - SCD Type 2
- OpTs (timestamp) - SCD Type 2

### Order Schema
- OrderId (string) - Primary Key
- CustId (string) - Foreign Key
- ItemName (string)
- PricePerUnit (double)
- Qty (integer)
- IsActive (boolean) - SCD Type 2
- StartDate (timestamp) - SCD Type 2
- EndDate (timestamp) - SCD Type 2
- OpTs (timestamp) - SCD Type 2

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest src/test/ -v --cov=src/main
```

## AWS Glue Job Parameters

Configure in `config/glue_params.yaml`:
- `--JOB_NAME`: Name of the Glue job
- `--customer_source_path`: S3 path for customer data
- `--order_source_path`: S3 path for order data
- `--curated_customer_path`: S3 output path for curated customer
- `--curated_order_path`: S3 output path for curated order
- `--analytics_path`: S3 output path for analytics
- `--glue_database`: Glue Catalog database name

## Running Locally

```bash
python src/main/job.py \
  --JOB_NAME local_test \
  --customer_source_path ./sample_data/customer/ \
  --order_source_path ./sample_data/order/
```

## AWS Glue Deployment

1. Upload code to S3:
```bash
aws s3 cp src/main/job.py s3://your-scripts-bucket/glue-jobs/
```

2. Create Glue job via AWS Console or CLI
3. Configure job parameters from `glue_params.yaml`

## Testing

Tests cover:
- Data ingestion from S3
- Data cleaning (NULL removal, deduplication)
- SCD Type 2 logic (IsActive, StartDate, EndDate, OpTs)
- Hudi upsert operations
- Aggregation calculations
- Schema validation

Run tests:
```bash
pytest src/test/test_job.py -v
```

## SCD Type 2 Implementation

Uses Apache Hudi with:
- **Operation**: UPSERT
- **Record Key**: CustId (customer), OrderId (order)
- **Precombine Field**: OpTs
- **Table Type**: COPY_ON_WRITE

Historical tracking:
- New records: IsActive=True, StartDate=current, EndDate=NULL
- Updated records: Old record IsActive=False, EndDate=current; New record IsActive=True
- Deleted records: IsActive=False, EndDate=current

## License
Proprietary - Internal Use Only