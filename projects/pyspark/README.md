# PySpark Data Processing Pipeline

## Overview
This PySpark project implements a data processing pipeline for customer order data with the following capabilities:
- Data ingestion from S3 sources
- Data cleaning (NULL removal, duplicate removal)
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spending calculation
- AWS Glue Catalog integration

## Data Sources
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Customer Data**: `s3://adif-sdlc/catalog/sdlc_wizard/customer/`
- **Output Path**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schema Details

### Customer Schema
- Customer
- name

### Order Schema
- ordersummary
- TotalAmount (Decimal type)

### SCD Type 2 Columns
- IsActive (boolean)
- StartDate (timestamp)
- EndDate (timestamp)
- OpTs (timestamp)

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

## AWS Permissions Required
- `s3:GetObject` on source buckets
- `s3:ListBucket` on source buckets
- `s3:PutObject` on output buckets
- Glue Catalog access

## Features
- **FR-INGEST-001**: Data ingestion from S3
- **FR-CLEAN-001**: Data cleaning (NULL/duplicate removal)
- **FR-SCD2-001**: SCD Type 2 implementation with Hudi
- **FR-AGG-001**: Customer aggregate spending calculation