# PySpark Data Processing Pipeline

## Overview
This PySpark project implements a data processing pipeline for customer and order data with the following capabilities:
- Data ingestion from S3 sources
- Data cleaning (NULL removal, duplicate removal)
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend calculation
- AWS Glue Catalog integration

## Data Sources
- Customer Data: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- Order Data: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Paths
- Cleaned Customer Data: `s3://adif-sdlc/cleaned/sdlc_wizard/customer/`
- Cleaned Order Data: `s3://adif-sdlc/cleaned/sdlc_wizard/order/`
- Curated Order Summary: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- Customer Aggregate Spend: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schema Details

### Customer Schema
- CustId: string
- Name: string
- EmailId: string
- Region: string

### Order Schema
- CustId: string
- Name: string
- EmailId: string
- Region: string
- OrderId: string

### SCD Type 2 Columns
- IsActive: boolean
- StartDate: timestamp
- EndDate: timestamp
- OpTs: timestamp

## Requirements
- Python 3.8+
- PySpark 3.3+
- Apache Hudi
- AWS Glue libraries
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
- s3:GetObject on source paths
- s3:PutObject on target paths
- glue:CreateTable
- glue:UpdateTable
- glue:GetTable