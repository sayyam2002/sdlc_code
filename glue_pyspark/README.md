# Customer Order Pipeline - AWS Glue PySpark Job

## Overview
This AWS Glue PySpark job implements a complete customer order data pipeline that:
- Ingests customer and order data from S3 CSV files
- Applies data quality rules (null removal, deduplication)
- Implements SCD Type 2 using Apache Hudi for customer dimension tracking
- Calculates customer aggregate spend analytics
- Registers tables in AWS Glue Data Catalog

## Architecture

### Data Flow
1. **Ingestion Layer**: Read CSV files from S3 raw zone
2. **Cleaning Layer**: Apply data quality transformations
3. **Curated Layer**: Write cleaned data to Glue Catalog
4. **SCD Layer**: Implement Type 2 slowly changing dimensions with Hudi
5. **Analytics Layer**: Generate aggregate spend metrics

### S3 Structure
- **Raw Zone**: `s3://adif-sdlc/sdlc_wizard/customerdata/`, `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Curated Zone**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Analytics Zone**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Prerequisites
- AWS Glue 3.0 or higher
- Python 3.7+
- Apache Hudi libraries (included in Glue 3.0+)
- IAM role with S3 and Glue Catalog permissions

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- Source paths for customer and order data
- Target paths for curated and analytics outputs
- Glue catalog database and table names
- Hudi configuration parameters

## Running the Job

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest src/test/test_job.py -v
```

### AWS Glue Deployment
```bash
# Upload job script
aws s3 cp src/main/job.py s3://adif-sdlc/scripts/customer_order_pipeline.py

# Upload config
aws s3 cp config/glue_params.yaml s3://adif-sdlc/config/glue_params.yaml

# Create Glue job
aws glue create-job \
  --name customer-order-pipeline \
  --role <GLUE_ROLE_ARN> \
  --command Name=glueetl,ScriptLocation=s3://adif-sdlc/scripts/customer_order_pipeline.py \
  --glue-version 3.0 \
  --default-arguments '{
    "--enable-glue-datacatalog": "true",
    "--enable-spark-ui": "true",
    "--spark-event-logs-path": "s3://adif-sdlc/logs/",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true"
  }'

# Run job
aws glue start-job-run --job-name customer-order-pipeline
```

## Data Schemas

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

### SCD Type 2 Schema (Hudi)
Adds tracking columns:
- IsActive (Boolean)
- StartDate (Timestamp)
- EndDate (Timestamp)
- OpTs (Timestamp)

## Testing
The test suite includes:
- Spark context initialization validation
- Data ingestion tests with mocked S3 reads
- Data cleaning transformation tests
- Schema validation tests
- SCD Type 2 logic tests
- Aggregation calculation tests

## Monitoring
- CloudWatch Logs: Job execution logs
- Glue Console: Job metrics and run history
- S3 Output: Verify data written to target paths

## Troubleshooting
- **S3 Access Denied**: Verify IAM role has s3:GetObject and s3:PutObject permissions
- **Schema Mismatch**: Check CSV headers match expected schema
- **Hudi Write Failures**: Ensure sufficient memory allocation (DPU >= 2)
- **Catalog Registration**: Verify database exists in Glue Data Catalog

## License
Proprietary - Internal Use Only