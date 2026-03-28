# AWS Glue PySpark Job - SDLC Wizard Data Processing

## Overview
This AWS Glue PySpark job processes customer and order data from the SDLC Wizard system, implementing data cleaning and SCD Type 2 historical tracking using Apache Hudi.

## Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

## Output Destinations
- **Curated Customer Data**: `s3://adif-sdlc/curated/sdlc_wizard/`
- **Curated Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Analytics Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schemas

### Customer Schema
- `CustId` (string): Customer identifier
- `Name` (string): Customer name
- `EmailId` (string): Customer email
- `Region` (string): Customer region

### Order Schema
- `OrderId` (string): Order identifier
- `ItemName` (string): Item name
- `PricePerUnit` (decimal): Price per unit
- `Qty` (integer): Quantity
- `Date` (string): Order date

### SCD Type 2 Columns (Added)
- `IsActive` (boolean): Current record indicator
- `StartDate` (timestamp): Record validity start
- `EndDate` (timestamp): Record validity end
- `OpTs` (timestamp): Operation timestamp

## Data Processing Steps

1. **Data Ingestion**: Read customer and order data from S3
2. **Data Cleaning**:
   - Remove NULL values
   - Remove 'Null' string values
   - Remove duplicate records
3. **SCD Type 2 Implementation**: Track historical changes using Hudi
4. **Data Output**: Write cleaned and historized data to curated/analytics zones

## Job Parameters
- `--JOB_NAME`: Glue job name
- `--customer_input_path`: Customer data S3 path (default: s3://adif-sdlc/sdlc_wizard/customerdata/)
- `--order_input_path`: Order data S3 path (default: s3://adif-sdlc/sdlc_wizard/orderdata/)
- `--customer_output_path`: Customer output S3 path (default: s3://adif-sdlc/curated/sdlc_wizard/)
- `--order_output_path`: Order output S3 path (default: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/)
- `--analytics_output_path`: Analytics output S3 path (default: s3://adif-sdlc/analytics/customeraggregatespend/)

## Running the Job

### AWS Glue Console
1. Upload `src/job.py` to S3
2. Create Glue job pointing to the script
3. Configure job parameters
4. Run the job

### AWS CLI
```bash
aws glue start-job-run \
  --job-name sdlc-wizard-data-processing \
  --arguments='--customer_input_path=s3://adif-sdlc/sdlc_wizard/customerdata/,--order_input_path=s3://adif-sdlc/sdlc_wizard/orderdata/'
```

## Testing
```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Requirements
- AWS Glue 4.0+
- Apache Hudi support
- Python 3.10+
- PySpark 3.3+