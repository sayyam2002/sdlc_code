# AWS Glue PySpark Job - Customer & Order Data Processing

## Overview
This AWS Glue PySpark job implements data ingestion, cleaning, and SCD Type 2 processing for customer and order data.

## Functional Requirements (FRD)
- **FR-INGEST-001**: Ingest customer and order data from S3 sources
- **FR-CLEAN-001**: Clean data by removing NULL values, 'Null' strings, and duplicates
- **FR-SCD2-001**: Implement SCD Type 2 with Hudi format for historical tracking
- **FR-AGGREGATE-001**: Aggregate customer spend data

## Technical Requirements (TRD)

### Data Sources
- Customer Data: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- Order Data: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- Customer Aggregate Spend: `s3://adif-sdlc/analytics/customeraggregatespend/`
- Order Summary: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`

### Glue Catalog
- **Database**: `gen_ai_poc_databrickscoe`
- **Tables**:
  - `sdlc_wizard_customer` → `s3://adif-sdlc/catalog/sdlc_wizard_customer/`
  - `sdlc_wizard_order` → `s3://adif-sdlc/catalog/sdlc_wizard_order/`

### Schema
**Customer Schema**:
- CustId (string)
- Name (string)
- EmailId (string)
- Region (string)

**Order Schema**:
- OrderId (string)
- CustId (string)
- OrderDate (string)
- Amount (double)

### SCD Type 2 Columns
- IsActive (boolean)
- StartDate (timestamp)
- EndDate (timestamp)
- OpTs (timestamp)

## Project Structure
```
glue_pyspark/
├── .gitignore
├── README.md
├── requirements.txt
├── config/
│   └── glue_params.yaml
├── src/
│   ├── __init__.py
│   ├── main/
│   │   ├── __init__.py
│   │   └── job.py
│   └── test/
│       ├── __init__.py
│       └── test_job.py
└── sample_data/
    ├── customerdata.csv
    └── orderdata.csv
```

## Installation

### Prerequisites
- Python 3.7+
- AWS CLI configured
- Access to S3 buckets and Glue Catalog

### Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration
Edit `config/glue_params.yaml` to customize job parameters:
- S3 paths
- Database names
- Table names
- Hudi configurations

## Running the Job

### Local Testing
```bash
# Run tests
pytest src/test/test_job.py -v

# Run with coverage
pytest src/test/test_job.py --cov=src/main --cov-report=html
```

### AWS Glue Deployment
```bash
# Upload to S3
aws s3 cp src/main/job.py s3://your-scripts-bucket/glue-jobs/

# Create Glue Job
aws glue create-job \
  --name customer-order-scd2-job \
  --role AWSGlueServiceRole \
  --command "Name=glueetl,ScriptLocation=s3://your-scripts-bucket/glue-jobs/job.py" \
  --default-arguments '{
    "--job-bookmark-option":"job-bookmark-enable",
    "--enable-metrics":"true",
    "--enable-continuous-cloudwatch-log":"true"
  }' \
  --max-capacity 10

# Run Glue Job
aws glue start-job-run --job-name customer-order-scd2-job
```

## Testing
The test suite includes 15+ test functions covering:
- Data ingestion from S3
- Data cleaning logic
- SCD Type 2 implementation
- Hudi upsert operations
- Schema validation
- Glue Catalog integration

## Monitoring
- CloudWatch Logs: `/aws-glue/jobs/output`
- CloudWatch Metrics: Custom metrics for record counts
- Job Bookmarks: Enabled for incremental processing

## Troubleshooting

### Common Issues
1. **S3 Access Denied**: Verify IAM role has S3 read/write permissions
2. **Glue Catalog Errors**: Ensure database exists and IAM role has Glue permissions
3. **Hudi Write Failures**: Check S3 path permissions and Hudi configuration

### Debug Mode
Set environment variable for verbose logging:
```bash
export SPARK_LOG_LEVEL=DEBUG
```

## Performance Tuning
- Adjust `--max-capacity` based on data volume
- Enable job bookmarks for incremental processing
- Use partitioning for large datasets
- Optimize Hudi configurations for write performance

## Security
- All S3 paths use encryption at rest
- IAM roles follow least privilege principle
- Sensitive data masked in logs

## License
Proprietary - Internal Use Only

## Contact
Data Engineering Team - dataeng@company.com