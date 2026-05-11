# AWS Glue PySpark Job - SDLC Wizard Data Pipeline

## Project Overview
This AWS Glue PySpark job implements a complete data pipeline for the SDLC Wizard project with:
- Data ingestion from multiple S3 sources
- Data cleaning and deduplication
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend calculations
- Order summary generation

## Functional Requirements Implemented

### FR-INGEST-001: Data Ingestion
- Ingests customer data from `s3://adif-sdlc/sdlc_wizard/customerdata/`
- Ingests order data from `s3://adif-sdlc/sdlc_wizard/orderdata/`
- Reads from Glue Catalog tables: `sdlc_wizard_customer`, `sdlc_wizard_order`

### FR-CLEAN-001: Data Cleaning
- Removes NULL values from critical fields
- Removes 'Null' string values
- Removes duplicate records based on primary keys

### FR-SCD2-001: Slowly Changing Dimension Type 2
- Tracks historical changes in customer data
- Adds SCD Type 2 columns: IsActive, StartDate, EndDate, OpTs
- Uses Apache Hudi for upsert operations

### FR-AGG-001: Customer Aggregate Spend
- Calculates total spend per customer
- Outputs to `s3://adif-sdlc/analytics/customeraggregatespend/`

### FR-SUMMARY-001: Order Summary
- Generates order summary statistics
- Outputs to `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`

## Technical Requirements Implemented

### TR-CLEAN-001: Data Cleaning Logic
- NULL value removal
- 'Null' string removal
- Duplicate record removal

### TR-SCD2-001: SCD Type 2 Implementation
- Hudi format with upsert operation
- Historical tracking with IsActive flag
- Timestamp management (StartDate, EndDate, OpTs)

### TR-AGG-001: Aggregation Logic
- Customer-level spend aggregation
- Order count calculations

## Architecture

```
S3 Sources
├── s3://adif-sdlc/sdlc_wizard/customerdata/
├── s3://adif-sdlc/sdlc_wizard/orderdata/
└── Glue Catalog Tables
    ├── sdlc_wizard_customer
    └── sdlc_wizard_order

Processing Layer (AWS Glue PySpark)
├── Data Ingestion
├── Data Cleaning
├── SCD Type 2 Processing (Hudi)
└── Aggregations

S3 Outputs
├── s3://adif-sdlc/catalog/sdlc_wizard_customer/ (SCD2)
├── s3://adif-sdlc/catalog/sdlc_wizard_order/ (Cleaned)
├── s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
└── s3://adif-sdlc/analytics/customeraggregatespend/
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
    ├── customerdata.csv
    └── orderdata.csv
```

## Setup Instructions

### Prerequisites
- AWS Account with Glue access
- Python 3.9+
- AWS CLI configured
- S3 buckets created with proper permissions

### Installation

1. **Clone/Create Project Structure**
```bash
mkdir -p glue_pyspark/{config,src/main,src/test,sample_data}
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure Parameters**
Edit `config/glue_params.yaml` with your AWS environment details.

### AWS Glue Job Configuration

**Job Properties:**
- Type: Spark
- Glue Version: 4.0
- Language: Python 3
- Worker Type: G.1X
- Number of Workers: 2
- Job Timeout: 60 minutes
- Max Concurrent Runs: 1

**IAM Role Permissions Required:**
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
        "arn:aws:s3:::adif-sdlc/*"
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

## Running the Job

### Local Testing
```bash
# Run tests
pytest src/test/test_job.py -v

# Run with coverage
pytest src/test/test_job.py --cov=src/main --cov-report=html
```

### AWS Glue Execution
```bash
# Upload job script
aws s3 cp src/main/job.py s3://adif-sdlc/scripts/

# Create Glue job
aws glue create-job \
  --name sdlc-wizard-pipeline \
  --role GlueServiceRole \
  --command "Name=glueetl,ScriptLocation=s3://adif-sdlc/scripts/job.py" \
  --default-arguments '{
    "--job-bookmark-option":"job-bookmark-enable",
    "--enable-metrics":"true",
    "--enable-continuous-cloudwatch-log":"true"
  }'

# Start job run
aws glue start-job-run --job-name sdlc-wizard-pipeline
```

## Data Schemas

### Customer Data Schema
```
customerdata/
├── customer_id (string) - Primary Key
├── customer_name (string)
├── email (string)
├── phone (string)
├── address (string)
├── city (string)
├── state (string)
├── zip_code (string)
├── registration_date (timestamp)
└── customer_status (string)
```

### Order Data Schema
```
orderdata/
├── order_id (string) - Primary Key
├── customer_id (string) - Foreign Key
├── order_date (timestamp)
├── order_amount (decimal)
├── order_status (string)
├── product_id (string)
├── quantity (integer)
└── payment_method (string)
```

### SCD Type 2 Customer Schema (Output)
```
catalog/sdlc_wizard_customer/
├── customer_id (string)
├── customer_name (string)
├── email (string)
├── phone (string)
├── address (string)
├── city (string)
├── state (string)
├── zip_code (string)
├── registration_date (timestamp)
├── customer_status (string)
├── IsActive (boolean) - SCD2
├── StartDate (timestamp) - SCD2
├── EndDate (timestamp) - SCD2
└── OpTs (timestamp) - SCD2
```

### Order Summary Schema (Output)
```
curated/sdlc_wizard/ordersummary/
├── customer_id (string)
├── total_orders (long)
├── total_amount (decimal)
├── avg_order_amount (decimal)
├── first_order_date (timestamp)
├── last_order_date (timestamp)
└── summary_date (timestamp)
```

### Customer Aggregate Spend Schema (Output)
```
analytics/customeraggregatespend/
├── customer_id (string)
├── customer_name (string)
├── total_spend (decimal)
├── order_count (long)
├── avg_spend_per_order (decimal)
└── calculation_date (timestamp)
```

## Monitoring and Logging

### CloudWatch Logs
- Log Group: `/aws-glue/jobs/sdlc-wizard-pipeline`
- Metrics: Job duration, records processed, errors

### Job Bookmarks
- Enabled for incremental processing
- Tracks processed data to avoid reprocessing

## Testing Strategy

### Unit Tests (15+ test functions)
1. `test_initialize_spark_contexts` - Spark initialization
2. `test_validate_s3_access` - S3 access validation
3. `test_read_customer_data` - Customer data ingestion
4. `test_read_order_data` - Order data ingestion
5. `test_clean_null_values` - NULL removal
6. `test_clean_null_strings` - 'Null' string removal
7. `test_remove_duplicates` - Deduplication
8. `test_add_scd2_columns` - SCD2 column addition
9. `test_scd2_upsert_new_records` - New record insertion
10. `test_scd2_upsert_changed_records` - Change tracking
11. `test_calculate_customer_aggregate_spend` - Aggregation
12. `test_generate_order_summary` - Summary generation
13. `test_write_data_safe` - Safe write operations
14. `test_end_to_end_pipeline` - Full pipeline test
15. `test_error_handling` - Error scenarios

## Troubleshooting

### Common Issues

**Issue: S3 Access Denied**
- Verify IAM role has GetObject/PutObject permissions
- Check bucket policies
- Verify bucket names are correct

**Issue: Glue Catalog Table Not Found**
- Ensure tables exist in Glue Catalog
- Verify database name: `sdlc_wizard`
- Check table names match configuration

**Issue: Hudi Write Failures**
- Verify Hudi libraries are available in Glue 4.0
- Check record key field exists in data
- Ensure precombine field is valid

**Issue: Memory Errors**
- Increase worker count
- Use larger worker type (G.2X)
- Optimize partition size

## Performance Optimization

1. **Partitioning**: Data partitioned by date for efficient querying
2. **Caching**: Frequently accessed DataFrames cached
3. **Broadcast Joins**: Small dimension tables broadcasted
4. **Predicate Pushdown**: Filters applied at source
5. **Columnar Format**: Parquet/Hudi for compression

## Maintenance

### Regular Tasks
- Monitor CloudWatch logs daily
- Review job metrics weekly
- Update sample data monthly
- Refresh Glue Catalog quarterly

### Version Control
- Tag releases: v1.0.0, v1.1.0, etc.
- Document changes in CHANGELOG.md
- Maintain backward compatibility

## Support

For issues or questions:
- Check CloudWatch logs first
- Review test cases for examples
- Consult AWS Glue documentation
- Contact data engineering team

## License

Internal use only - ADIF SDLC Project

## Contributors

- Data Engineering Team
- SDLC Wizard Project Team

---

**Last Updated**: 2024
**Version**: 1.0.0