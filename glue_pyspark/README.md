# AWS Glue PySpark ETL Pipeline - Customer Order Analytics

## Overview
This AWS Glue PySpark job implements a complete ETL pipeline for customer and order data processing with the following capabilities:
- Ingests customer and order CSV data from S3
- Performs data quality operations (null removal, deduplication)
- Registers cleaned data to AWS Glue Data Catalog
- Implements SCD Type 2 using Apache Hudi for customer baseline tracking
- Generates customer aggregate spend analytics
- Implements incremental processing with change data capture

## Architecture
```
S3 Source (CSV) → Glue Job → Data Cleaning → Catalog Registration → Hudi SCD2 → Analytics Aggregation → S3 Target
```

## Data Flow
1. **Ingestion**: Load customer and order CSV files from S3
2. **Cleaning**: Remove nulls and duplicates
3. **Catalog**: Register tables in Glue Data Catalog
4. **Baseline**: Maintain customer baseline with SCD Type 2 (Hudi)
5. **Analytics**: Calculate customer aggregate spend
6. **Output**: Write results to S3 in Parquet format

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
    ├── customer_sample.csv
    └── order_sample.csv
```

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- S3 input/output paths
- Glue catalog database and table names
- Hudi configuration
- Processing options

## Prerequisites
- AWS Glue 3.0 or higher
- Python 3.7+
- Apache Hudi libraries
- IAM role with permissions for S3, Glue Catalog, and CloudWatch

## Installation
```bash
pip install -r requirements.txt
```

## Running the Job

### Local Testing
```bash
python -m pytest src/test/test_job.py -v
```

### AWS Glue Deployment
1. Upload `src/main/job.py` to S3
2. Create Glue job with script location
3. Configure job parameters from `glue_params.yaml`
4. Run the job

## Sample Data
Sample CSV files are provided in `sample_data/` directory:
- `customer_sample.csv`: Customer master data
- `order_sample.csv`: Order transaction data

## Key Features
- **Bulletproof S3 Access**: Validates S3 paths before operations
- **Safe Read/Write**: Wrapped operations with error handling
- **Column Normalization**: Lowercase column names for consistency
- **SCD Type 2**: Historical tracking with IsActive, StartDate, EndDate
- **Incremental Processing**: CDC pattern for efficient updates
- **Comprehensive Testing**: Full test coverage with mocked I/O

## Output Tables
1. **customer** (Curated): Cleaned customer data
2. **order** (Curated): Cleaned order data
3. **customer_baseline** (State): SCD Type 2 customer history
4. **ordersummary** (Curated): Order summary with customer details
5. **customeraggregatespend** (Analytics): Customer spending aggregates

## Monitoring
- CloudWatch logs for job execution
- Glue job metrics for performance monitoring
- Data quality metrics in job output

## Troubleshooting
- Check CloudWatch logs for detailed error messages
- Verify S3 bucket permissions
- Ensure Glue catalog database exists
- Validate Hudi library availability

## License
Proprietary - Internal Use Only