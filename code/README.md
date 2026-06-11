# AWS Glue ETL Pipeline - Customer Order SCD Type 2 Processing

## Overview

This AWS Glue ETL pipeline implements a complete data processing solution for customer and order data with SCD Type 2 historical tracking using Apache Hudi. The pipeline ingests CSV files from S3, performs data quality operations, implements slowly changing dimension logic, and generates aggregated analytics.

## Features

- **Data Ingestion**: Reads customer and order CSV files from S3
- **Data Quality**: Removes null values and duplicates
- **SCD Type 2**: Maintains historical records using Apache Hudi
- **Incremental Processing**: Detects and processes customer updates
- **Aggregation**: Generates customer spend analytics
- **Catalog Integration**: Registers all tables in AWS Glue Data Catalog

## Architecture

### Data Flow
1. Ingest customer data from `s3://adif-sdlc/sdlc_wizard/customerdata/`
2. Ingest order data from `s3://adif-sdlc/sdlc_wizard/orderdata/`
3. Clean data (remove nulls and duplicates)
4. Join customer and order data
5. Apply SCD Type 2 logic to ordersummary table
6. Generate customer aggregate spend analytics
7. Register all tables in Glue Data Catalog

### Tables
- **sdlc_wizard_customer**: Cleaned customer data
- **sdlc_wizard_order**: Cleaned order data
- **ordersummary**: SCD Type 2 table with historical tracking (Hudi format)
- **customeraggregatespend**: Aggregated customer spend by date

## Prerequisites

- AWS Glue 3.0 or later
- Apache Hudi support enabled
- IAM role with permissions for:
  - S3 read/write access
  - Glue Data Catalog operations
  - CloudWatch logging
- Glue Data Catalog database: `gen_ai_poc_databrickscoe`

## Configuration

Edit `config/glue_params.yaml` to customize:
- Source and target S3 paths
- Database and table names
- Hudi configuration parameters
- Processing options

## Deployment

### AWS Glue Job Parameters
```
--enable-glue-datacatalog: true
--datalake-formats: hudi
--enable-spark-ui: true
--enable-job-insights: true
--enable-metrics: true
```

### Recommended Glue Job Settings
- **Glue Version**: 3.0 or later
- **Worker Type**: G.1X or G.2X
- **Number of Workers**: 2-5 (adjust based on data volume)
- **Job Timeout**: 2880 minutes
- **Max Concurrent Runs**: 1 (for SCD2 consistency)

## Running the Job

### Via AWS Glue Console
1. Upload `src/main/job.py` to S3
2. Create a new Glue job pointing to the script
3. Configure job parameters from `config/glue_params.yaml`
4. Run the job

### Via AWS CLI
```bash
aws glue start-job-run --job-name <job-name>
```

## Testing

Run unit tests locally:
```bash
pytest src/test/test_job.py -v
```

## Monitoring

- **CloudWatch Logs**: Monitor job execution and errors
- **CloudWatch Metrics**: Track job performance
- **Glue Job Insights**: Analyze job execution details
- **Athena**: Query processed tables for validation

## Data Quality

The pipeline implements the following data quality rules:
- Removes records with string "Null" (case-sensitive)
- Removes records with actual NULL values
- Removes duplicate records
- Validates schema compatibility
- Logs data quality metrics

## SCD Type 2 Logic

The ordersummary table maintains historical records:
- **IsActive**: Boolean flag for current records
- **StartDate**: When the record became active
- **EndDate**: When the record was superseded (null for active)
- **OpTs**: Operation timestamp for tracking

## Troubleshooting

### Common Issues
1. **Hudi write failures**: Ensure Glue 3.0+ and proper Hudi configuration
2. **Permission errors**: Verify IAM role permissions
3. **Schema mismatches**: Check CSV file headers match expected schema
4. **Memory errors**: Increase worker count or worker type

### Logs
Check CloudWatch Logs for detailed error messages and execution traces.

## License

Proprietary - Internal Use Only