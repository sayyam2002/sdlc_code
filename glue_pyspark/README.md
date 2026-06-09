# AWS Glue PySpark ETL Pipeline - Customer Order Analytics

## Overview
This AWS Glue PySpark job implements a complete ETL pipeline for customer and order data processing with SCD Type 2 support using Apache Hudi.

## Features
- **Data Ingestion**: Load customer and order CSV files from S3
- **Data Cleaning**: Remove NULL values, "Null" strings, and duplicate records
- **Data Cataloging**: Write cleaned data to Glue Data Catalog in Parquet format
- **SCD Type 2**: Track historical changes using Apache Hudi
- **Aggregation**: Calculate customer aggregate spending analytics
- **Incremental Processing**: Detect and process only changed records

## Architecture
```
S3 Raw Data → Ingestion → Cleaning → Cataloging → SCD2 (Hudi) → Aggregation → Analytics
```

## Data Flow
1. **Ingest**: Read customer and order CSV files from S3
2. **Clean**: Remove nulls and duplicates
3. **Catalog**: Write to Parquet and register in Glue Catalog
4. **Join & SCD2**: Merge datasets with historical tracking
5. **Aggregate**: Calculate customer spending metrics

## Schemas

### Customer Schema
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| CustId | String | No | Customer unique identifier |
| Name | String | No | Customer full name |
| EmailId | String | No | Customer email address |
| Region | String | No | Customer geographic region |

### Order Schema
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| OrderId | String | No | Order unique identifier |
| ItemName | String | No | Product/item name |
| PricePerUnit | Decimal(10,2) | No | Unit price of item |
| Qty | Integer | No | Quantity ordered |
| Date | Date | No | Order date |
| CustId | String | No | Customer identifier (FK) |

### Order Summary Schema (SCD2)
| Column | Type | Description |
|--------|------|-------------|
| CustId | String | Customer identifier |
| OrderId | String | Order identifier |
| Name | String | Customer name |
| EmailId | String | Customer email |
| Region | String | Customer region |
| ItemName | String | Product name |
| PricePerUnit | Decimal(10,2) | Unit price |
| Qty | Integer | Quantity |
| Date | Date | Order date |
| IsActive | Boolean | Current record flag |
| StartDate | Timestamp | Record valid from |
| EndDate | Timestamp | Record valid to |
| OpTs | Timestamp | Operation timestamp |

## Configuration
All parameters are defined in `config/glue_params.yaml`:
- S3 source paths for customer and order data
- S3 target paths for curated data
- Glue catalog database and table names
- Hudi configuration parameters

## Installation
```bash
pip install -r requirements.txt
```

## Usage

### AWS Glue Job
Deploy as AWS Glue job with Python Shell or Spark environment.

### Local Testing
```bash
python -m pytest src/test/test_job.py -v
```

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

## Key Components

### job.py
- `initialize_spark_contexts()`: Initialize Spark and Glue contexts
- `get_job_parameters()`: Load configuration from YAML
- `validate_s3_access()`: Verify S3 bucket accessibility
- `read_data_safe()`: Safe data reading with error handling
- `write_data_safe()`: Safe data writing with error handling
- `clean_dataframe()`: Remove nulls and duplicates
- `apply_scd2()`: Implement SCD Type 2 logic
- `aggregate_customer_spend()`: Calculate spending metrics

### test_job.py
- Comprehensive unit tests with mocked Spark I/O
- Sample data validation
- Transformation logic verification
- Error handling tests

## AWS Glue Job Parameters
When deploying to AWS Glue, the job reads parameters from `glue_params.yaml` which can be overridden by Glue job parameters.

## Error Handling
- S3 access validation before operations
- Graceful handling of missing data
- Comprehensive logging for debugging
- Safe read/write operations with fallbacks

## Performance Considerations
- Parquet format for efficient storage
- Hudi for incremental updates
- Partition strategies for large datasets
- Broadcast joins for small dimension tables

## Monitoring
- CloudWatch logs integration
- Spark UI metrics
- Custom application logs

## License
Proprietary - Internal Use Only