# AWS Glue PySpark ETL Job - Customer Order Processing

## Overview
This AWS Glue job processes customer and order data from S3, performs data quality transformations, registers tables in the Glue Data Catalog, and generates customer spending aggregates.

## Features
- **Data Ingestion**: Load customer and order CSV files from S3
- **Data Cleaning**: Remove NULL values, "Null" strings, and duplicate records
- **Catalog Registration**: Register cleaned datasets in AWS Glue Data Catalog
- **Aggregation**: Generate customer-level spending analytics
- **SCD Type 2**: Track historical changes with IsActive, StartDate, EndDate, OpTs columns
- **Hudi Integration**: Use Apache Hudi format for upsert operations

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
- S3 source paths for customer and order data
- S3 target paths for cleaned, curated, and analytics data
- Glue Data Catalog database and table names
- File formats and processing options

## Running the Job

### AWS Glue Console
1. Upload `src/main/job.py` to S3
2. Create a new Glue job pointing to the script
3. Configure job parameters from `glue_params.yaml`
4. Run the job

### Local Testing
```bash
pip install -r requirements.txt
python -m pytest src/test/test_job.py -v
```

## Data Flow
1. **Ingest**: Read customer and order CSV files from S3
2. **Clean**: Remove nulls and duplicates
3. **Catalog**: Register tables in Glue Data Catalog
4. **Aggregate**: Join and compute customer spending totals
5. **Write**: Output to S3 in Parquet format with SCD Type 2 metadata

## Sample Data
Sample CSV files are provided in `sample_data/` for testing:
- `customers.csv`: Customer master data (CustId, Name, EmailId, Region)
- `orders.csv`: Order transaction data (OrderId, ItemName, PricePerUnit, Qty, Date, CustId)

## Requirements
- AWS Glue 3.0+
- Python 3.7+
- PySpark 3.1+
- Apache Hudi (included in Glue 3.0+)

## Schema Definitions

### Customer Schema
| Column | Type | Nullable |
|--------|------|----------|
| CustId | String | No |
| Name | String | No |
| EmailId | String | No |
| Region | String | No |

### Order Schema
| Column | Type | Nullable |
|--------|------|----------|
| OrderId | String | No |
| ItemName | String | No |
| PricePerUnit | Decimal(10,2) | No |
| Qty | Integer | No |
| Date | Date | No |
| CustId | String | No |

### SCD Type 2 Columns (Added to Output)
| Column | Type | Description |
|--------|------|-------------|
| IsActive | Boolean | Current record indicator |
| StartDate | Timestamp | Record effective start date |
| EndDate | Timestamp | Record effective end date |
| OpTs | Timestamp | Operation timestamp |

## License
Proprietary - Internal Use Only