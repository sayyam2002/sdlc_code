# Customer Order Pipeline - AWS Glue PySpark ETL Job

## Overview
This AWS Glue ETL job implements a complete customer order data pipeline that:
- Ingests customer and order data from S3 CSV files
- Performs data quality transformations (null removal, deduplication)
- Writes cleaned data to AWS Glue Data Catalog in Parquet format
- Implements SCD Type 2 using Apache Hudi for order summary tracking
- Generates customer aggregate spend analytics

## Project Structure
```
glue_pyspark/
├── README.md
├── requirements.txt
├── config/
│   └── glue_params.yaml
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   └── job.py
│   └── test/
│       ├── __init__.py
│       └── test_job.py
└── sample_data/
    ├── customer.csv
    └── order.csv
```

## Technical Requirements Implemented

### TR-INGEST-001: Customer Data Ingestion
- Reads CSV files from S3: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- Schema: CustId, Name, EmailId, Region
- Validates schema and handles errors

### TR-INGEST-002: Order Data Ingestion
- Reads CSV files from S3: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- Schema: OrderId, ItemName, PricePerUnit, Qty, Date, CustId
- Type casting for numeric and date fields

### TR-CLEAN-001: Null Value Removal
- Removes records with NULL values
- Removes records with string literal "Null"

### TR-CLEAN-002: Deduplication
- Removes duplicate records based on all columns

### TR-CATALOG-001 & TR-CATALOG-002: Data Catalog Registration
- Writes cleaned data to S3 in Parquet format
- Registers tables in AWS Glue Data Catalog:
  - `sdlc_wizard_customer`
  - `sdlc_wizard_order`

### SCD Type 2 Implementation
- Implements Slowly Changing Dimension Type 2 using Apache Hudi
- Tracks historical changes with IsActive, StartDate, EndDate, OpTs columns
- Writes to: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`

### Aggregation Analytics
- Generates customer aggregate spend table
- Calculates total spending per customer per date
- Writes to: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Configuration

All runtime parameters are defined in `config/glue_params.yaml`:
- Source paths for customer and order data
- Target paths for curated and analytics data
- Database and table names
- CSV parsing options

## Running the Job

### AWS Glue Console
1. Upload `src/main/job.py` to S3
2. Create a new Glue job pointing to the script
3. Configure job parameters from `glue_params.yaml`
4. Run the job

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest src/test/test_job.py -v
```

## Sample Data
Sample CSV files are provided in `sample_data/` directory:
- `customer.csv`: Sample customer records
- `order.csv`: Sample order records

## Dependencies
- PySpark (provided by AWS Glue)
- PyYAML for configuration management
- pytest for testing

## Error Handling
- Schema validation with detailed error messages
- Type casting error handling
- Empty file warnings
- S3 access validation before operations

## Logging
Comprehensive logging at INFO, WARNING, and ERROR levels:
- Record counts at each stage
- Schema validation results
- Data quality metrics
- Processing timestamps

## IAM Permissions Required
- s3:GetObject on source buckets
- s3:PutObject on target buckets
- glue:CreateTable, glue:UpdateTable
- logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents