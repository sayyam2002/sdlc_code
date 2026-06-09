# AWS Glue PySpark ETL Job - Customer Order Processing

## Overview
This AWS Glue PySpark job implements a complete ETL pipeline for customer and order data processing with the following capabilities:
- Ingests customer and order CSV data from S3
- Performs data quality transformations (null removal, deduplication)
- Registers cleaned data in AWS Glue Data Catalog
- Implements SCD Type 2 logic using Apache Hudi
- Tracks historical changes with IsActive, StartDate, EndDate, OpTs columns

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

## Prerequisites
- AWS Glue 3.0 or higher
- Python 3.7+
- Apache Hudi libraries (included in Glue 3.0+)
- S3 bucket access: s3://adif-sdlc/

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- Input/output S3 paths
- Database and table names
- File formats
- Hudi configuration options

## Running the Job

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest src/test/test_job.py -v
```

### AWS Glue Deployment
1. Upload job script to S3:
   ```bash
   aws s3 cp src/main/job.py s3://adif-sdlc/scripts/customer_order_etl.py
   ```

2. Create Glue job via AWS Console or CLI:
   ```bash
   aws glue create-job \
     --name customer-order-etl \
     --role <your-glue-role> \
     --command Name=glueetl,ScriptLocation=s3://adif-sdlc/scripts/customer_order_etl.py \
     --glue-version 3.0 \
     --worker-type G.1X \
     --number-of-workers 2
   ```

3. Run the job:
   ```bash
   aws glue start-job-run --job-name customer-order-etl
   ```

## Data Flow
1. **Ingest**: Read customer and order CSV files from S3
2. **Clean**: Remove NULL/'Null' values and duplicates
3. **Catalog**: Write cleaned data to Parquet and register in Glue Catalog
4. **Join & SCD**: Join datasets and apply SCD Type 2 logic using Hudi
5. **Output**: Write ordersummary table with historical tracking

## Sample Data
Sample CSV files are provided in `sample_data/` directory for testing:
- `customer.csv`: Sample customer records
- `order.csv`: Sample order records

## Technical Requirements Implemented
- TR-INGEST-001: Load customer CSV from S3
- TR-INGEST-002: Load order CSV from S3
- TR-CLEAN-001: Remove nulls from customer data
- TR-CLEAN-002: Remove nulls from order data
- TR-CLEAN-003: Deduplicate customer records
- TR-CLEAN-004: Deduplicate order records
- TR-CATALOG-001: Catalog customer data
- TR-CATALOG-002: Catalog order data
- FR-CATALOG-003: SCD Type 2 with Hudi
- TR-INCR-001: Incremental processing

## Monitoring
- CloudWatch logs available under `/aws-glue/jobs/logs-v2/`
- Job metrics tracked in Glue console
- Data quality metrics logged during execution

## Troubleshooting
- Ensure IAM role has S3 read/write permissions
- Verify Glue Data Catalog database exists
- Check S3 paths are accessible
- Review CloudWatch logs for detailed error messages