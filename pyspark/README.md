# PySpark Data Processing Pipeline

## Project Overview
This PySpark project implements a data processing pipeline for customer and order data with the following capabilities:
- Data ingestion from S3 sources
- Data cleaning (NULL removal, duplicate removal)
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend analytics
- AWS Glue Catalog integration

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schema Details

### Customer Table
- CustId: String
- Name: String
- EmailId: String
- Region: String

### Order Table
- Order: Table (TODO: Complete schema details from TRD)

### SCD Type 2 Columns
- IsActive: Boolean
- StartDate: Timestamp
- EndDate: Timestamp
- OpTs: Timestamp

## Requirements
- Python 3.8+
- PySpark 3.3+
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

## AWS Permissions Required
- `s3:GetObject` for source buckets
- `s3:ListBucket` for source buckets
- `s3:PutObject` for target buckets
- `glue:CreateTable`
- `glue:UpdateTable`
- `glue:GetTable`

## Data Cleaning Rules (TR-CLEAN-001)
- Remove NULL values
- Remove 'Null' string values
- Remove duplicate records