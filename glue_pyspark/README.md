# AWS Glue PySpark ETL Job - Customer Order Processing

## Overview
This AWS Glue PySpark job implements a complete ETL pipeline for customer and order data processing with the following capabilities:
- Ingests customer and order CSV data from S3
- Performs data quality transformations (null removal, deduplication)
- Implements SCD Type 2 using Apache Hudi for customer dimension tracking
- Calculates customer aggregate spending analytics
- Registers tables in AWS Glue Data Catalog for Athena querying

## Architecture

### Data Flow
1. **Ingestion**: Read customer and order CSV files from S3
2. **Cleaning**: Remove null values ("Null" strings and actual NULLs) and duplicates
3. **Catalog Registration**: Write cleaned data to Glue Data Catalog
4. **SCD Type 2**: Detect customer changes and maintain history using Hudi
5. **Aggregation**: Join customer and order data, calculate spending metrics
6. **Output**: Write results to S3 and Glue Data Catalog

### S3 Paths
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Curated Output**: `s3://adif-sdlc/curated/*`
- **Customer State**: `s3://adif-sdlc/state/customer_previous/`
- **Analytics Output**: `s3://adif-sdlc/analytics/customeraggregatespend/`

### Glue Catalog Tables
- `sdlc_wizard_customer` - Cleaned customer data
- `sdlc_wizard_order` - Cleaned order data
- `ordersummary` - SCD Type 2 customer dimension (Hudi format)
- `customeraggregatespend` - Customer spending aggregations

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
    ├── customer_sample.csv
    └── order_sample.csv
```

## Configuration

### glue_params.yaml
Contains all configurable parameters:
- S3 input/output paths
- Database and table names
- Schema definitions
- Hudi configuration
- Processing options

## Schemas

### Customer Schema
| Column | Type | Nullable |
|--------|------|----------|
| CustId | String | No |
| Name | String | Yes |
| EmailId | String | Yes |
| Region | String | Yes |

### Order Schema
| Column | Type | Nullable |
|--------|------|----------|
| OrderId | String | No |
| CustId | String | No |
| OrderDate | String | Yes |
| Amount | String | Yes |

### SCD Type 2 Schema (ordersummary)
Adds tracking columns:
- `IsActive` (Boolean)
- `StartDate` (Timestamp)
- `EndDate` (Timestamp)
- `OpTs` (Timestamp)

## Running the Job

### AWS Glue Console
1. Upload `src/main/job.py` to S3
2. Create Glue job pointing to the script
3. Configure job parameters from `glue_params.yaml`
4. Run the job

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest src/test/test_job.py -v
```

## Data Quality Rules
- Remove records with "Null" string literal (case-sensitive)
- Remove records with actual NULL values
- Remove duplicate records (full row comparison)
- Validate schema on read

## SCD Type 2 Logic
- **Initial Load**: All records created as active (IsActive=true, EndDate=null)
- **Change Detection**: Compare all attributes except CustId
- **Update Process**:
  - Expire changed records (IsActive=false, EndDate=current timestamp)
  - Insert new versions (IsActive=true, StartDate=current timestamp)
- **State Management**: Previous customer data stored in `s3://adif-sdlc/state/customer_previous/`

## Aggregation Logic
- Join customer and order data on CustId
- Calculate total spending per customer per date
- Output to analytics S3 location and Glue catalog

## Monitoring
- CloudWatch logs automatically captured by Glue
- Job metrics available in Glue console
- Athena queries enabled via catalog registration

## Dependencies
- PySpark (provided by Glue)
- AWS Glue libraries (provided by Glue)
- PyYAML for configuration parsing

## Testing
Comprehensive test suite covering:
- Spark context initialization
- Data ingestion with schema validation
- Null and duplicate removal
- Column normalization
- SCD Type 2 transformations
- Aggregation logic
- Catalog registration

Tests use mocked Spark I/O to avoid real filesystem operations.

## Notes
- All timestamps use UTC timezone
- OpTs uses job execution start time
- Hudi format enables time-travel queries in Athena
- Incremental processing requires state management