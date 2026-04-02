# AWS Glue PySpark Job - Customer Order Data Processing

## Overview
This AWS Glue job processes customer and order data with the following capabilities:
- Data ingestion from S3 sources
- Data cleaning (NULL removal, duplicate removal)
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend calculation
- Glue Catalog integration

## Project Structure
```
glue_pyspark/
├── src/
│   ├── __init__.py
│   ├── main/
│   │   └── job.py          # Main Glue job
│   └── test/
│       └── test_job.py     # Comprehensive test suite
├── requirements.txt
├── README.md
└── .gitignore
```

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Data Outputs
- **Curated Customer**: `s3://adif-sdlc/curated/sdlc_wizard/customer/`
- **Curated Order**: `s3://adif-sdlc/curated/sdlc_wizard/order/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schemas

### Customer Schema
- CustId: String
- Name: String
- EmailId: String
- Region: String
- IsActive: Boolean (SCD Type 2)
- StartDate: Timestamp (SCD Type 2)
- EndDate: Timestamp (SCD Type 2)
- OpTs: Timestamp (SCD Type 2)

### Order Schema
- OrderId: String
- ItemName: String
- PricePerUnit: Double
- Qty: Integer
- Date: Date
- IsActive: Boolean (SCD Type 2)
- StartDate: Timestamp (SCD Type 2)
- EndDate: Timestamp (SCD Type 2)
- OpTs: Timestamp (SCD Type 2)

## Requirements
- Python 3.9+
- PySpark 3.3+
- AWS Glue 4.0
- Apache Hudi

## Installation
```bash
pip install -r requirements.txt
```

## Testing
```bash
pytest src/test/test_job.py -v
```

## Glue Job Parameters
- `--JOB_NAME`: Name of the Glue job
- `--customer_input_path`: S3 path for customer data (default: s3://adif-sdlc/sdlc_wizard/customerdata/)
- `--order_input_path`: S3 path for order data (default: s3://adif-sdlc/sdlc_wizard/orderdata/)
- `--customer_output_path`: S3 path for curated customer data (default: s3://adif-sdlc/curated/sdlc_wizard/customer/)
- `--order_output_path`: S3 path for curated order data (default: s3://adif-sdlc/curated/sdlc_wizard/order/)
- `--order_summary_output_path`: S3 path for order summary (default: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/)
- `--aggregate_spend_output_path`: S3 path for aggregate spend (default: s3://adif-sdlc/analytics/customeraggregatespend/)
- `--glue_database`: Glue Catalog database name (default: sdlc_wizard_db)

## Features
1. **Data Cleaning**: Removes NULL values, 'Null' strings, and duplicates
2. **SCD Type 2**: Tracks historical changes using Hudi format
3. **Aggregations**: Calculates customer total spend
4. **Glue Catalog Integration**: Registers tables for Athena queries