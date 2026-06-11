# AWS Glue Data Pipeline - Customer Order Processing

## Overview

This AWS Glue job implements a complete data pipeline for processing customer and order data with the following features:

- Ingests customer and order CSV files from S3
- Performs data cleaning (null removal and deduplication)
- Registers cleaned data in AWS Glue Data Catalog
- Implements SCD Type 2 logic using Apache Hudi for order summary
- Generates customer aggregate spend analytics
- Supports incremental processing for customer updates

## Architecture

### Data Flow

1. **Ingestion**: Read customer and order CSV files from S3
2. **Cleaning**: Remove null values and duplicates
3. **Catalog Registration**: Register cleaned data as Glue tables
4. **SCD Type 2**: Join customer and order data, apply SCD Type 2 logic using Hudi
5. **Aggregation**: Calculate customer aggregate spend
6. **Catalog Registration**: Register output tables for Athena queries

### AWS Services Used

- **Amazon S3**: Data storage
- **AWS Glue**: ETL processing with Spark and Hudi
- **AWS Glue Data Catalog**: Metadata management
- **Amazon Athena**: Query engine (for registered tables)
- **Amazon CloudWatch**: Monitoring and logging

## Project Structure

```
.
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
    └── input.csv
```

## Configuration

All configuration parameters are defined in `config/glue_params.yaml`:

- Source paths for customer and order data
- Target paths for curated and analytics data
- Glue Data Catalog database and table names
- Hudi configuration parameters

## Running the Job

### Prerequisites

- AWS Glue 3.0 or higher
- IAM role with permissions for S3, Glue Data Catalog, and CloudWatch
- Glue database `gen_ai_poc_databrickscoe` must exist

### Glue Job Configuration

- **Job Type**: Glue ETL (Spark)
- **Glue Version**: 3.0 or higher
- **Language**: Python 3
- **Worker Type**: G.1X or G.2X
- **Number of Workers**: 2-5 (adjust based on data volume)
- **Job Parameters**: All parameters have defaults from config file

### Execution

The job can run without any runtime arguments using default configuration values from `config/glue_params.yaml`.

Optional runtime arguments:
- `--customer_source_path`: Override customer data source path
- `--order_source_path`: Override order data source path
- `--catalog_database`: Override Glue catalog database name
- `--ordersummary_target_path`: Override order summary target path
- `--customeraggregatespend_target_path`: Override aggregate spend target path

## Testing

Run unit tests locally:

```bash
python -m pytest src/test/test_job.py -v
```

Tests cover:
- Data ingestion
- Null value removal
- Deduplication
- SCD Type 2 logic
- Aggregation calculations
- Catalog registration

## Data Schemas

### Customer Data
- CustId: string
- Name: string
- EmailId: string
- Region: string

### Order Data
- OrderId: string
- ItemName: string
- PricePerUnit: string
- Qty: string
- Date: string
- CustId: string

### Order Summary (SCD Type 2)
- All fields from customer and order join
- IsActive: boolean
- StartDate: timestamp
- EndDate: timestamp (nullable)
- OpTs: timestamp

### Customer Aggregate Spend
- Name: string
- TotalAmount: double
- Date: string

## Monitoring

Monitor job execution via CloudWatch:
- Job execution logs
- Record counts at each stage
- Data quality metrics
- Error and warning messages

## Notes

- The job implements SCD Type 2 using Apache Hudi for historical tracking
- All cleaned data is registered in Glue Data Catalog for Athena queries
- Incremental processing is supported for customer updates
- Business key for SCD Type 2: CustId + OrderId