# PySpark Customer Order Analytics Pipeline

## Overview
This PySpark project implements a data pipeline for customer order analytics with SCD Type 2 historical tracking using Apache Hudi.

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`

## Output Destinations
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`
- **BRD Use Case 1**: `s3://adif-sdlc/brd_usecase1/`

## Glue Catalog
- **Database**: `read`
- **Table**: `customeraggregatespend`

## Features
- Data ingestion from S3
- Data cleaning (NULL removal, duplicate removal)
- Customer-Order join operations
- SCD Type 2 implementation with Hudi
- Aggregate spend calculation per customer
- AWS Glue Catalog integration

## Requirements
- Python 3.7+
- PySpark 3.0+
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

## AWS Glue Configuration
- **Glue Version**: 3.0 or higher
- **Worker Type**: Standard or G.1X
- **IAM Role**: Requires S3 read permissions for `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Schema Details

### Customer Schema
- CustId: String
- Name: String
- EmailId: String
- Region: String

### Order Schema
- OrderId: String
- ItemName: String
- PricePerUnit: Decimal/Double
- Qty: Integer

### SCD Type 2 Columns
- IsActive: Boolean
- StartDate: Timestamp
- EndDate: Timestamp
- OpTs: Timestamp

## Testing
```bash
pytest tests/ -v
```

## Project Structure
```
projects/pyspark/
├── src/
│   ├── __init__.py
│   └── job.py
├── tests/
│   ├── __init__.py
│   └── test_job.py
├── data/
│   └── sample.csv
├── requirements.txt
├── README.md
└── .gitignore