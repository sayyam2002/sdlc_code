# Customer Order Analytics ETL - AWS Glue PySpark Job

## Project Overview
This AWS Glue PySpark job implements a Customer Order Analytics ETL pipeline that:
- Extracts customer and order data from S3 CSV files
- Validates data quality with comprehensive error logging
- Transforms data by deriving full names, joining datasets, and calculating order counts
- Loads processed data to S3 in Parquet format with partitioning

## Architecture
- **Platform**: AWS Glue
- **Language**: PySpark
- **Input Format**: CSV
- **Output Format**: Parquet
- **Error Logging**: CSV to S3

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
- Input/output S3 paths
- Data formats
- Error logging paths
- Job execution parameters

## Data Flow
1. **Extract**: Load customers.csv and orders.csv from S3
2. **Validate**: Check for nulls, duplicates, data types; log errors
3. **Transform**:
   - Derive FULL_NAME (title case)
   - Join customers and orders on CID
   - Calculate ORDER_COUNT per customer
   - Add PROCESSED_DATE timestamp
4. **Load**: Write to S3 Parquet partitioned by PROCESSED_DATE

## Schemas

### Input: customers.csv
| Column | Type | Constraints |
|--------|------|-------------|
| CID | Integer | NOT NULL, > 0 |
| FNAME | String | NOT NULL, Non-empty |
| LNAME | String | NOT NULL, Non-empty |

### Input: orders.csv
| Column | Type | Constraints |
|--------|------|-------------|
| OID | Integer | NOT NULL, > 0 |
| CID | Integer | NOT NULL, > 0 |
| ONAME | String | NOT NULL, Non-empty |

### Output: customer_orders (Parquet)
| Column | Type | Description |
|--------|------|-------------|
| CID | Integer | Customer ID |
| FNAME | String | First Name |
| LNAME | String | Last Name |
| FULL_NAME | String | Derived: Title case full name |
| OID | Integer | Order ID |
| ONAME | String | Order Name |
| ORDER_COUNT | Long | Total orders per customer |
| PROCESSED_DATE | Timestamp | Job execution timestamp |

## Running the Job

### AWS Glue Console
1. Upload job.py to S3
2. Upload glue_params.yaml to S3
3. Create Glue job with:
   - Type: Spark
   - Glue version: 3.0+
   - Language: Python 3
   - Script location: s3://path/to/job.py
   - DPU: 5-10 (recommended)

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest src/test/test_job.py -v
```

## Error Handling
- Invalid records logged to: `s3://adif-sdlc/logs/errors/{entity}/run_date={YYYY-MM-DD}/`
- Errors include: null values, invalid data types, duplicates
- Job continues processing valid records after logging errors

## Sample Data
Sample CSV files are provided in `sample_data/` for testing:
- `customers.csv`: 5 sample customer records
- `orders.csv`: 8 sample order records

## Dependencies
- PySpark (provided by AWS Glue)
- PyYAML
- pytest (for testing)

## Monitoring
- CloudWatch Logs: Monitor job execution and errors
- S3 Error Logs: Review rejected records
- Job Metrics: Track DPU usage and execution time

## Version
- **Version**: 1.0
- **Date**: 2024-01-15
- **Related FRD/TRD**: 1.0