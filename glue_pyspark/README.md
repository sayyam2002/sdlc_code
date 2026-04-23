# AWS Glue PySpark ETL Job

## Overview
Production-ready AWS Glue PySpark job implementing:
- Data ingestion from multiple sources
- Data quality validation and cleansing
- SCD Type 2 implementation using Apache Hudi
- Comprehensive error handling and logging
- Full test coverage with pytest

## Architecture

### Data Flow
```
S3 Raw Data → Glue Job → Data Validation → Transformation → Hudi SCD Type 2 → S3 Curated
                ↓
         Glue Catalog Registration
```

### Components
- **job.py**: Main ETL orchestration
- **glue_params.yaml**: Configuration management
- **test_job.py**: Comprehensive test suite

## Requirements

### System Requirements
- Python 3.9+
- Apache Spark 3.3+
- AWS Glue 4.0
- Apache Hudi 0.12+

### Python Dependencies
```
pyspark>=3.3.0
boto3>=1.26.0
pytest>=7.4.0
pytest-mock>=3.11.0
pyyaml>=6.0
```

## Configuration

### glue_params.yaml
```yaml
source_paths:
  customer_data: s3://data-lake-raw/customers/
  order_data: s3://data-lake-raw/orders/

target_paths:
  curated_customer: s3://data-lake-curated/customers/
  curated_order: s3://data-lake-curated/orders/

glue_catalog:
  database: curated_db
  customer_table: dim_customer
  order_table: fact_order
```

## Usage

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest src/test/ -v --cov=src/main --cov-report=html

# Run job locally (with local Spark)
python src/main/job.py
```

### AWS Glue Deployment
```bash
# Upload to S3
aws s3 cp src/main/job.py s3://glue-scripts/jobs/
aws s3 cp config/glue_params.yaml s3://glue-scripts/config/

# Create Glue Job
aws glue create-job \
  --name customer-order-etl \
  --role GlueServiceRole \
  --command "Name=glueetl,ScriptLocation=s3://glue-scripts/jobs/job.py" \
  --default-arguments '{
    "--config_path":"s3://glue-scripts/config/glue_params.yaml",
    "--enable-glue-datacatalog":"true",
    "--enable-metrics":"true",
    "--enable-spark-ui":"true",
    "--spark-event-logs-path":"s3://glue-logs/spark-events/"
  }' \
  --glue-version "4.0" \
  --number-of-workers 10 \
  --worker-type "G.1X"

# Run Glue Job
aws glue start-job-run --job-name customer-order-etl
```

## Features

### 1. Data Ingestion (FR-INGEST-001)
- Multi-source data reading (CSV, Parquet, JSON)
- S3 path validation before read
- Schema enforcement and validation
- Automatic column name normalization

### 2. Data Quality (FR-CLEAN-001)
- Null value handling
- Duplicate record detection and removal
- Data type validation
- Business rule validation
- Invalid record quarantine

### 3. SCD Type 2 (FR-SCD2-001)
- Apache Hudi integration
- Automatic history tracking
- IsActive flag management
- StartDate/EndDate tracking
- OpTs (operation timestamp) tracking

### 4. Error Handling
- Comprehensive try-catch blocks
- S3 access validation
- Schema mismatch detection
- Data quality failure handling
- Detailed error logging

### 5. Monitoring & Logging
- Structured logging with timestamps
- Job metrics tracking
- Record count validation
- Performance monitoring
- CloudWatch integration

## Testing

### Test Coverage
- Unit tests for all transformations
- Integration tests for data flows
- Mock AWS services (S3, Glue Catalog)
- Schema validation tests
- SCD Type 2 logic tests
- Error handling tests

### Run Tests
```bash
# All tests
pytest src/test/ -v

# With coverage
pytest src/test/ --cov=src/main --cov-report=html

# Specific test
pytest src/test/test_job.py::test_customer_data_ingestion -v

# With markers
pytest src/test/ -m "integration" -v
```

## Project Structure
```
glue_pyspark/
├── .gitignore
├── README.md
├── requirements.txt
├── pytest.ini
├── config/
│   └── glue_params.yaml
└── src/
    ├── __init__.py
    ├── main/
    │   ├── __init__.py
    │   └── job.py
    └── test/
        ├── __init__.py
        └── test_job.py
```

## Troubleshooting

### Common Issues

**Issue**: S3 Access Denied
```
Solution: Verify IAM role has s3:GetObject, s3:PutObject permissions
```

**Issue**: Glue Catalog Table Not Found
```
Solution: Ensure database exists and job has glue:CreateTable permission
```

**Issue**: Hudi Write Failure
```
Solution: Check S3 path permissions and Hudi configuration
```

## Performance Tuning

### Spark Configuration
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
```

### Hudi Optimization
```python
hudi_options = {
    "hoodie.compact.inline": "true",
    "hoodie.compact.inline.max.delta.commits": "5",
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained": "10"
}
```

## Monitoring

### CloudWatch Metrics
- Job duration
- Records processed
- Data quality failures
- S3 read/write operations

### Glue Job Metrics
- DPU hours consumed
- Success/failure rate
- Data processed (GB)

## Security

### IAM Permissions Required
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake-raw/*",
        "arn:aws:s3:::data-lake-curated/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable"
      ],
      "Resource": "*"
    }
  ]
}
```

## Contributing
1. Follow PEP 8 style guide
2. Add tests for new features
3. Update documentation
4. Run full test suite before commit

## License
Proprietary - Internal Use Only

## Support
Contact: data-engineering-team@company.com