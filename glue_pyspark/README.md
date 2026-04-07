# AWS Glue PySpark Job - Customer Order SCD Type 2 Processing

## Overview
This AWS Glue PySpark job implements a complete ETL pipeline for customer and order data processing with SCD Type 2 (Slowly Changing Dimension) tracking using Apache Hudi format.

## Functional Requirements Implemented

### FR-INGEST-001: Data Ingestion
- **Customer Data Source**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data Source**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- Reads data from S3 buckets in various formats (CSV, Parquet, JSON)

### FR-CLEAN-001: Data Cleaning
- Removes NULL values from critical fields
- Removes 'Null' string values
- Removes duplicate records based on primary keys

### FR-SCD2-001: SCD Type 2 Implementation
- Tracks historical changes using Hudi format
- Adds tracking columns: `IsActive`, `StartDate`, `EndDate`, `OpTs`
- Implements upsert operation for incremental updates
- Maintains complete history of customer changes

### FR-AGGREGATE-001: Order Aggregation
- Aggregates orders by customer name
- Calculates total order amount per customer
- Includes order date information

### FR-OUTPUT-001: Data Output
- **Customer Snapshot**: `s3://adif-sdlc/curated/sdlc_wizard/customer_snapshot/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Curated Customer**: `s3://adif-sdlc/curated/sdlc_wizard/customer/`
- **Curated Order**: `s3://adif-sdlc/curated/sdlc_wizard/order/`
- **Analytics**: `s3://adif-sdlc/analytics/`

## Technical Architecture

### Data Schemas

#### Customer Schema
```
CustId: string
Name: string
EmailId: string
Region: string
```

#### Order Schema
```
CustId: string
Name: string
EmailId: string
Region: string
OrderId: string
```

#### Order Summary Schema
```
Name: string
TotalAmount: decimal
Date: date
```

#### SCD Type 2 Tracking Columns
```
IsActive: boolean
StartDate: timestamp
EndDate: timestamp
OpTs: timestamp
```

## Project Structure
```
glue_pyspark/
├── config/
│   └── glue_params.yaml          # Job configuration parameters
├── src/
│   ├── __init__.py
│   ├── main/
│   │   ├── __init__.py
│   │   └── job.py                # Main ETL job logic
│   └── test/
│       ├── __init__.py
│       └── test_job.py           # Comprehensive unit tests
├── sample_data/
│   ├── customerdata.csv          # Sample customer data
│   └── orderdata.csv             # Sample order data
├── .gitignore
├── requirements.txt
└── README.md
```

## Configuration

### Glue Job Parameters (glue_params.yaml)
- **source_customer_path**: S3 path for customer data
- **source_order_path**: S3 path for order data
- **output_customer_snapshot_path**: S3 path for customer snapshots
- **output_order_summary_path**: S3 path for order summaries
- **output_curated_customer_path**: S3 path for curated customer data
- **output_curated_order_path**: S3 path for curated order data
- **output_analytics_path**: S3 path for analytics data
- **temp_path**: S3 path for temporary data
- **glue_database**: Glue Catalog database name
- **glue_table_customer**: Customer table name
- **glue_table_order**: Order table name

## Setup Instructions

### Prerequisites
- Python 3.7+
- AWS Glue 3.0+
- Apache Spark 3.1+
- Apache Hudi 0.10+

### Installation
```bash
pip install -r requirements.txt
```

### Running Tests
```bash
pytest src/test/test_job.py -v --cov=src/main --cov-report=html
```

### Deploying to AWS Glue
1. Upload code to S3:
```bash
aws s3 cp src/main/job.py s3://your-scripts-bucket/glue-jobs/
aws s3 cp config/glue_params.yaml s3://your-scripts-bucket/glue-jobs/config/
```

2. Create Glue Job:
```bash
aws glue create-job \
  --name customer-order-scd2-job \
  --role AWSGlueServiceRole \
  --command "Name=glueetl,ScriptLocation=s3://your-scripts-bucket/glue-jobs/job.py" \
  --default-arguments '{
    "--job-language":"python",
    "--enable-glue-datacatalog":"true",
    "--enable-continuous-cloudwatch-log":"true",
    "--enable-spark-ui":"true",
    "--spark-event-logs-path":"s3://adif-sdlc/spark-logs/",
    "--TempDir":"s3://adif-sdlc/temp/",
    "--config_path":"s3://your-scripts-bucket/glue-jobs/config/glue_params.yaml"
  }' \
  --glue-version "3.0" \
  --number-of-workers 10 \
  --worker-type "G.1X"
```

## AWS IAM Permissions Required

### S3 Permissions
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::adif-sdlc/*",
    "arn:aws:s3:::adif-sdlc"
  ]
}
```

### Glue Permissions
```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetDatabase",
    "glue:GetTable",
    "glue:CreateTable",
    "glue:UpdateTable",
    "glue:GetPartitions"
  ],
  "Resource": "*"
}
```

## Monitoring and Logging

### CloudWatch Logs
- Log group: `/aws-glue/jobs/customer-order-scd2-job`
- Metrics: Job duration, records processed, errors

### Spark UI
- Available during job execution
- Access via Glue Console

## Data Quality Checks
- NULL value validation
- Duplicate detection
- Schema validation
- Record count validation
- SCD Type 2 integrity checks

## Performance Optimization
- Partitioning by date for time-series data
- Bucketing by customer ID for joins
- Broadcast joins for small dimension tables
- Hudi indexing for fast upserts
- Dynamic partition pruning

## Troubleshooting

### Common Issues
1. **OutOfMemoryError**: Increase worker memory or number of workers
2. **S3 Access Denied**: Verify IAM role permissions
3. **Schema Mismatch**: Check source data schema matches expected schema
4. **Hudi Write Failures**: Verify temp directory has sufficient space

## Testing Coverage
- Unit tests: 15+ test functions
- Integration tests: End-to-end pipeline validation
- Mock AWS services: S3, Glue Catalog
- Code coverage: >85%

## Maintenance
- Regular dependency updates
- Schema evolution handling
- Performance monitoring
- Cost optimization reviews

## Support
For issues or questions, contact the data engineering team.

## License
Proprietary - Internal Use Only