# SDLC Wizard Customer-Order Data Pipeline

## Overview
AWS Glue PySpark job for processing customer and order data with SCD Type 2 implementation using Apache Hudi.

## Functional Requirements (FRD)
- **FR-INGEST-001**: Ingest customer data from S3 CSV sources
- **FR-INGEST-002**: Ingest order data from S3 sources
- **FR-CLEAN-001**: Remove NULL values from datasets
- **FR-CLEAN-002**: Remove 'Null' string values
- **FR-CLEAN-003**: Remove duplicate records
- **FR-SCD2-001**: Implement SCD Type 2 with IsActive, StartDate, EndDate, OpTs columns
- **FR-HUDI-001**: Use Apache Hudi format for upsert operations
- **FR-CATALOG-001**: Register tables in AWS Glue Catalog

## Technical Requirements (TRD)

### Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Curated Output**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Analytics Output**: `s3://adif-sdlc/analytics/`

### Schemas
**Customer Schema**:
- CustId (String)
- Name (String)
- EmailId (String)
- Region (String)

**Order Schema**:
- CustId (String)
- OrderId (String)

**SCD Type 2 Columns**:
- IsActive (Boolean)
- StartDate (Timestamp)
- EndDate (Timestamp)
- OpTs (Timestamp)

### Glue Catalog
- **Database**: `sdlc_wizard_db`

## Project Structure
```
glue_pyspark/
├── config/
│   └── glue_params.yaml          # Runtime parameters
├── src/
│   ├── __init__.py
│   ├── main/
│   │   └── job.py                # Main Glue job
│   └── test/
│       └── test_job.py           # Unit tests
├── requirements.txt
└── README.md
```

## Installation

### Local Development
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### AWS Glue Deployment
Upload `job.py` and `glue_params.yaml` to S3, then create Glue job pointing to the script.

## Configuration

Edit `config/glue_params.yaml`:
```yaml
job:
  name: "sdlc-wizard-customer-order-pipeline"
  log_level: "INFO"

inputs:
  customer_source_path: "s3://adif-sdlc/sdlc_wizard/customerdata/"
  order_source_path: "s3://adif-sdlc/sdlc_wizard/orderdata/"
  source_format: "csv"

outputs:
  curated_target_path: "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
  analytics_target_path: "s3://adif-sdlc/analytics/"
  write_mode: "append"

glue_catalog:
  database_name: "sdlc_wizard_db"
  customer_table_name: "customer_data"
  order_table_name: "order_data"

runtime:
  watermark: "2024-01-01T00:00:00Z"

flags:
  enable_scd2: true
  enable_hudi: true
  enable_data_cleaning: true
```

## Running the Job

### AWS Glue
```bash
aws glue start-job-run --job-name sdlc-wizard-customer-order-pipeline \
  --arguments='--customer_source_path=s3://adif-sdlc/sdlc_wizard/customerdata/,--order_source_path=s3://adif-sdlc/sdlc_wizard/orderdata/'
```

### Local Testing
```bash
pytest glue_pyspark/src/test/test_job.py -v
```

## Features

### Data Ingestion
- CSV parsing with header detection
- Explicit schema enforcement
- Type safety validation

### Data Cleaning
- NULL value removal
- 'Null' string removal
- Duplicate record elimination

### SCD Type 2 Implementation
- Historical change tracking
- Active/inactive record management
- Temporal validity (StartDate/EndDate)
- Operation timestamp tracking

### Apache Hudi Integration
- Upsert operations
- Record-level updates
- Time travel capabilities

### AWS Glue Catalog Integration
- Automatic table registration
- Metadata management
- Athena query support

## Testing
- 15+ unit tests covering all FRD requirements
- Mock AWS services (S3, Glue Catalog)
- Schema validation tests
- SCD Type 2 logic verification
- Data cleaning validation

## Monitoring
- CloudWatch logging for file paths and row counts
- Error tracking and alerting
- Job execution metrics

## License
Proprietary - ADIF SDLC Project