# AWS Glue PySpark ETL Job - Customer Order Processing

## Overview
This AWS Glue PySpark job implements a complete ETL pipeline for customer and order data processing with the following capabilities:
- Ingests customer and order CSV data from S3
- Performs data quality checks and deduplication
- Implements SCD Type 2 using Apache Hudi
- Generates customer aggregate spend analytics
- Registers tables in AWS Glue Data Catalog

## Architecture
- **Source Data**: S3 CSV files (customers, orders)
- **Processing**: AWS Glue PySpark with Apache Hudi
- **Target Storage**: S3 (Parquet/Hudi format)
- **Catalog**: AWS Glue Data Catalog
- **Query Engine**: Amazon Athena

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
    ├── customers.csv
    └── orders.csv
```

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- Source S3 paths for customer and order data
- Target S3 paths for curated and analytics data
- Glue Data Catalog database and table names
- Hudi configuration parameters

## Data Flow
1. **Ingestion**: Read customer and order CSV files from S3
2. **Cleaning**: Remove NULL values and duplicates
3. **Catalog Registration**: Register cleaned data in Glue Data Catalog
4. **SCD Type 2**: Maintain historical customer changes using Hudi
5. **Aggregation**: Calculate customer aggregate spend
6. **Analytics Catalog**: Register analytics tables for Athena queries

## Schemas

### Customer Schema
| Column | Type | Nullable |
|--------|------|----------|
| CustId | string | No |
| Name | string | Yes |
| EmailId | string | Yes |
| Region | string | Yes |

### Order Schema
| Column | Type | Nullable |
|--------|------|----------|
| OrderId | string | No |
| ItemName | string | Yes |
| PricePerUnit | double | Yes |
| Qty | integer | Yes |
| Date | string | Yes |
| CustId | string | No |

### Order Summary (Hudi SCD Type 2)
Includes all order columns plus:
- IsActive (boolean)
- StartDate (timestamp)
- EndDate (timestamp)
- OpTs (timestamp)

### Customer Aggregate Spend
| Column | Type |
|--------|------|
| CustId | string |
| TotalSpend | double |

## Running the Job

### AWS Glue Console
1. Upload job.py to S3
2. Create Glue job with Python 3.9 and Glue 3.0+
3. Configure job parameters from glue_params.yaml
4. Run the job

### Local Testing
```bash
pip install -r requirements.txt
python -m pytest src/test/test_job.py -v
```

## Error Handling
- Validates S3 paths before operations
- Logs detailed error messages for troubleshooting
- Raises exceptions for critical failures
- Implements retry logic for transient errors

## Monitoring
- CloudWatch Logs for job execution details
- Glue job metrics for performance monitoring
- Data quality metrics logged during processing

## Dependencies
- PySpark 3.x
- AWS Glue libraries
- Apache Hudi
- PyYAML for configuration management

## Sample Data
Sample CSV files are provided in `sample_data/` directory for testing:
- `customers.csv`: Sample customer records
- `orders.csv`: Sample order records

## Notes
- Ensure IAM role has permissions for S3, Glue Data Catalog, and CloudWatch
- Hudi tables require Glue 3.0 or higher
- Athena queries on Hudi tables require Athena engine version 2 or higher