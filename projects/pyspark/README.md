# PySpark Data Processing Pipeline

## Overview
This PySpark project implements a data processing pipeline for customer and order data with the following capabilities:
- Data ingestion from S3 sources
- Data cleaning (NULL and 'Null' string removal)
- Deduplication
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend calculation
- AWS Glue Catalog integration

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Locations
- **Curated Data**: `s3://adif-sdlc/curated/sdlc_wizard/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Analytics**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Glue Catalog
- **Database**: `Full`

## Schemas

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
- Date: String

### SCD Type 2 Columns
- IsActive: Boolean
- StartDate: Timestamp
- EndDate: Timestamp
- OpTs: Timestamp

## Requirements
- Python 3.8+
- PySpark 3.x
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

## Features
1. **Data Ingestion**: Read from S3 sources
2. **Data Cleaning**: Remove NULL and 'Null' string values
3. **Deduplication**: Remove duplicate records
4. **SCD Type 2**: Track historical changes using Hudi
5. **Aggregation**: Calculate customer aggregate spend
6. **Glue Integration**: Register tables in AWS Glue Catalog