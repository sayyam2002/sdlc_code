# PySpark Customer and Order Data Processing Pipeline

## Overview
This PySpark project implements a data processing pipeline for customer and order data with the following capabilities:
- Ingests customer and order data from S3
- Performs data cleaning (removes NULLs, 'Null' strings, duplicates)
- Implements SCD Type 2 using Apache Hudi
- Writes curated data to S3 in Parquet/Hudi format
- Registers tables in AWS Glue Catalog

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Paths
- **Curated Customer**: `s3://adif-sdlc/curated/sdlc_wizard/customer/`
- **Curated Order**: `s3://adif-sdlc/curated/sdlc_wizard/order/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schema

### Customer Schema
- CustId: String
- Name: String
- EmailId: String
- Region: String

### Order Schema
- OrderId: String
- ItemName: String
- PricePerUnit: Decimal
- Qty: Integer
- Date: Date
- CustId: String

### SCD Type 2 Columns (added to Customer)
- IsActive: Boolean
- StartDate: Timestamp
- EndDate: Timestamp
- OpTs: Timestamp

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