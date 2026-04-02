# AWS Glue PySpark ETL Job - Customer Order Analytics with SCD Type 2

## Overview
This AWS Glue ETL job processes customer and order data with SCD Type 2 tracking using Apache Hudi format.

## Project Structure
```
glue_pyspark/
├── config/
│   └── glue_params.yaml          # Runtime configuration
├── src/
│   ├── __init__.py
│   ├── main/
│   │   └── job.py                # Main Glue job
│   └── test/
│       └── test_job.py           # Comprehensive tests
├── requirements.txt
└── README.md
```

## Data Sources (from TRD)
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Curated Customer**: `s3://adif-sdlc/curated/sdlc_wizard/customer/`
- **Curated Order**: `s3://adif-sdlc/curated/sdlc_wizard/order/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Glue Catalog
- **Database**: `gen_ai_poc_databrickscoe`
- **Tables**:
  - `sdlc_wizard_customer`
  - `sdlc_wizard_order`
  - `ordersummary`
  - `customeraggregatespend`

## Schemas (from TRD)

### Customer Schema
- CustId (String) - Primary Key
- Name (String)
- EmailId (String)
- Region (String)
- IsActive (Boolean) - SCD Type 2
- StartDate (Timestamp) - SCD Type 2
- EndDate (Timestamp) - SCD Type 2
- OpTs (Timestamp) - SCD Type 2

### Order Schema
- OrderId (String) - Primary Key
- CustId (String) - Foreign Key
- ItemName (String)
- PricePerUnit (Decimal)
- Qty (Integer)
- IsActive (Boolean) - SCD Type 2
- StartDate (Timestamp) - SCD Type 2
- EndDate (Timestamp) - SCD Type 2
- OpTs (Timestamp) - SCD Type 2

## Features
1. **Data Ingestion**: Read from S3 raw zones
2. **Data Cleaning**: Remove NULLs, 'Null' strings, duplicates
3. **SCD Type 2**: Track historical changes with Hudi upsert
4. **Aggregations**: Customer aggregate spend calculation
5. **Glue Catalog Integration**: Register tables for querying

## Requirements
- AWS Glue 3.0+
- Python 3.7+
- Apache Hudi
- IAM Role with S3 read/write permissions

## Local Development

### Setup
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Run Tests
```bash
pytest glue_pyspark/src/test/test_job.py -v --tb=short
```

## AWS Glue Deployment

### Job Parameters (from glue_params.yaml)
```yaml
job:
  name: customer-order-scd2-etl
  log_level: INFO

inputs:
  customer_source_path: s3://adif-sdlc/sdlc_wizard/customerdata/
  order_source_path: s3://adif-sdlc/sdlc_wizard/orderdata/
  source_format: parquet

outputs:
  curated_customer_path: s3://adif-sdlc/curated/sdlc_wizard/customer/
  curated_order_path: s3://adif-sdlc/curated/sdlc_wizard/order/
  ordersummary_path: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
  customeraggregatespend_path: s3://adif-sdlc/analytics/customeraggregatespend/
  write_mode: append

glue_catalog:
  database: gen_ai_poc_databrickscoe
  customer_table: sdlc_wizard_customer
  order_table: sdlc_wizard_order
  ordersummary_table: ordersummary
  customeraggregatespend_table: customeraggregatespend

hudi:
  customer_recordkey: CustId
  order_recordkey: OrderId
  precombine_field: OpTs
  table_type: COPY_ON_WRITE
  operation: upsert

runtime:
  watermark: "2024-01-01T00:00:00Z"

flags:
  enable_scd2: true
  enable_data_cleaning: true
  enable_aggregations: true
```

### Create Glue Job
```bash
aws glue create-job \
  --name customer-order-scd2-etl \
  --role <your-glue-role> \
  --command Name=glueetl,ScriptLocation=s3://<bucket>/scripts/job.py \
  --glue-version 3.0 \
  --worker-type G.1X \
  --number-of-workers 2 \
  --default-arguments '{
    "--job-bookmark-option": "job-bookmark-enable",
    "--enable-metrics": "",
    "--enable-continuous-cloudwatch-log": "true"
  }'
```

## Testing Strategy
- **15+ test functions** covering:
  - Data ingestion from S3
  - Schema validation
  - Data cleaning logic
  - SCD Type 2 operations
  - Hudi upsert functionality
  - Aggregation calculations
  - Glue Catalog registration
  - Error handling

## Monitoring
- CloudWatch Logs: `/aws-glue/jobs/customer-order-scd2-etl`
- Metrics: Job duration, records processed, errors

## License
Proprietary - Internal Use Only