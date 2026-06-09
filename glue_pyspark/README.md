# AWS Glue PySpark Job - Customer Order Data Processing

## Overview
This AWS Glue PySpark job processes customer and order data from S3, performs data quality checks, creates catalog tables, implements SCD Type 2 using Apache Hudi, and generates customer aggregate spend analytics.

## Features
- **Data Ingestion**: Load customer and order CSV files from S3
- **Data Quality**: Remove NULL values, 'Null' strings, and duplicates
- **Catalog Integration**: Register cleaned datasets in AWS Glue Data Catalog
- **SCD Type 2**: Track historical changes using Apache Hudi
- **Aggregation**: Calculate customer total spending for analytics

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

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- Input/output S3 paths
- Data formats
- Glue catalog database and table names
- Hudi configuration for SCD Type 2

## Sample Data
Sample CSV files are provided in `sample_data/`:
- `customer.csv`: Customer master data (CustId, Name, EmailId, Region)
- `order.csv`: Order transaction data (OrderId, ItemName, PricePerUnit, Qty, Date, CustId)

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

## Dependencies
- PySpark 3.x
- AWS Glue libraries
- Apache Hudi (for SCD Type 2)
- PyYAML (for configuration)

## Technical Requirements Implemented
- **TR-INGEST-001**: Customer CSV ingestion from S3
- **TR-INGEST-002**: Order CSV ingestion from S3
- **TR-DQ-001**: Data quality validation and cleansing
- **TR-CATALOG-001**: Glue Data Catalog registration
- **TR-INCR-001**: SCD Type 2 implementation with Hudi
- **TR-AGG-001**: Customer aggregate spend calculation

## Output Datasets
1. **Catalog Tables**: `customer` and `order` in Glue Data Catalog
2. **Order Summary (Hudi)**: SCD Type 2 table at `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
3. **Customer Aggregate Spend**: Analytics table at `s3://adif-sdlc/analytics/customeraggregatespend/`

## Notes
- All S3 paths are validated before operations
- Column names are normalized to lowercase
- Comprehensive error handling and logging included
- Tests use mocked Spark I/O for safe execution