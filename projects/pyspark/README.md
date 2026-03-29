# PySpark Customer Order Data Processing Pipeline

## Overview
This PySpark application processes customer and order data with SCD Type 2 implementation using Apache Hudi format. It performs data ingestion, cleaning, transformation, and aggregation operations.

## Data Sources
- Customer Data: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- Order Data: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- Customer Aggregate Spend: `s3://adif-sdlc/analytics/customeraggregatespend/`
- Athena Results: `s3://adif-sdlc/athena-results/`

## Output
- Order Summary (Hudi SCD Type 2): `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`

## Features
- Data ingestion from S3
- Data cleaning (NULL and 'Null' string removal)
- Duplicate record removal
- SCD Type 2 implementation with Hudi
- AWS Glue Catalog integration
- Comprehensive error handling and logging

## Schema

### Customer Data
- CustId: string
- Name: string
- EmailId: string
- Region: string

### Order Data
- CustId: string
- OrderId: string

### SCD Type 2 Columns
- IsActive: boolean
- StartDate: timestamp
- EndDate: timestamp
- OpTs: timestamp

## Requirements
- Python 3.8+
- PySpark 3.x
- Apache Hudi
- AWS Glue
- boto3

## Installation
```bash
pip install -r requirements.txt
```

## Usage
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.12.0 \
  src/job.py
```

## Testing
```bash
pytest tests/ -v
```

## Configuration
- Encoding: UTF-8
- Storage Format: Parquet (Hudi)
- Hudi Operation: UPSERT