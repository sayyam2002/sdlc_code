# AWS Glue PySpark ETL Job - Customer Order Summary with SCD Type 2

## Project Overview
This AWS Glue ETL job implements a complete data pipeline for customer and order data processing with SCD Type 2 historical tracking using Apache Hudi.

## Functional Requirements (FRD)
- **FR-INGEST-001**: Ingest customer and order data from S3
- **FR-CLEAN-001**: Clean data (remove NULLs, 'Null' strings, duplicates)
- **FR-SCD2-001**: Implement SCD Type 2 with Hudi format
- **FR-AGG-001**: Aggregate customer spending data
- **FR-CATALOG-001**: Register tables in AWS Glue Catalog

## Technical Requirements (TRD)

### Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/*`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/*`

### Data Targets
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`
- **Order Summary (Curated)**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Athena Results**: `s3://adif-sdlc/athena-results/`

### Schemas
**Customer Schema:**
- CustId (string)
- Name (string)
- EmailId (string)
- Region (string)

**Order Schema:**
- OrderId (string)
- ItemName (string)
- PricePerUnit (double)
- Qty (int)
- Date (string)
- CustId (string)

### SCD Type 2 Columns (Hudi)
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

## Setup Instructions

### Prerequisites
- Python 3.9+
- AWS CLI configured
- Access to S3 buckets: `s3://adif-sdlc/`
- AWS Glue permissions

### Installation
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running Tests
```bash
# Run all tests
pytest src/test/ -v

# Run with coverage
pytest src/test/ --cov=src/main --cov-report=html

# Run specific test
pytest src/test/test_job.py::test_customer_data_ingestion -v
```

## AWS Glue Job Configuration

### Job Parameters
```yaml
--JOB_NAME: customer_order_etl_scd2
--CUSTOMER_INPUT_PATH: s3://adif-sdlc/sdlc_wizard/customerdata/
--ORDER_INPUT_PATH: s3://adif-sdlc/sdlc_wizard/orderdata/
--AGGREGATE_OUTPUT_PATH: s3://adif-sdlc/analytics/customeraggregatespend/
--CURATED_OUTPUT_PATH: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
--ATHENA_RESULTS_PATH: s3://adif-sdlc/athena-results/
--GLUE_DATABASE: sdlc_wizard_db
--ENABLE_SPARK_UI: true
--TempDir: s3://adif-sdlc/temp/
```

### Glue Job Settings
- **Type**: Spark
- **Glue Version**: 4.0
- **Language**: Python 3
- **Worker Type**: G.1X
- **Number of Workers**: 5
- **Job Timeout**: 60 minutes
- **Max Retries**: 1

## Data Flow

1. **Ingestion** (FR-INGEST-001)
   - Read customer data from S3
   - Read order data from S3
   - Validate S3 paths and access

2. **Data Cleaning** (FR-CLEAN-001)
   - Remove NULL values
   - Remove 'Null' string values
   - Remove duplicate records

3. **SCD Type 2 Implementation** (FR-SCD2-001)
   - Add IsActive, StartDate, EndDate, OpTs columns
   - Use Hudi upsert operation
   - Track historical changes

4. **Aggregation** (FR-AGG-001)
   - Calculate total spend per customer
   - Join customer and order data
   - Generate customer aggregate spend

5. **Catalog Registration** (FR-CATALOG-001)
   - Register tables in Glue Catalog
   - Database: sdlc_wizard_db

## Monitoring & Logging
- CloudWatch Logs: `/aws-glue/jobs/customer_order_etl_scd2`
- Spark UI: Enabled via job parameters
- Custom metrics logged for each transformation step

## Troubleshooting

### Common Issues
1. **S3 Access Denied**: Verify IAM role has S3 read/write permissions
2. **Hudi Write Failures**: Check temp directory permissions
3. **Schema Mismatch**: Verify input data matches expected schemas

### Debug Mode
Set environment variable for verbose logging:
```bash
export GLUE_DEBUG=true
```

## Performance Optimization
- Partition by Region for customer data
- Partition by Date for order data
- Use broadcast joins for small dimension tables
- Enable dynamic partition pruning

## Security
- All S3 paths use server-side encryption (SSE-S3)
- IAM role-based access control
- No hardcoded credentials
- VPC endpoints for S3 access

## Maintenance
- Review CloudWatch logs weekly
- Monitor Glue job metrics
- Update Hudi version as needed
- Validate data quality metrics

## Contact
For issues or questions, contact the Data Engineering team.