# AWS Glue PySpark Job - Customer Order Analytics with SCD Type 2

## Overview
This AWS Glue job processes customer and order data with the following capabilities:
- Ingests data from multiple S3 sources
- Implements data quality checks (NULL removal, deduplication)
- Applies SCD Type 2 (Slowly Changing Dimension) using Apache Hudi
- Generates customer aggregate spend analytics
- Registers tables in AWS Glue Data Catalog

## Data Sources (S3 Paths)
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- **Curated Customer**: `s3://adif-sdlc/curated/sdlc_wizard/customer/`
- **Curated Order**: `s3://adif-sdlc/curated/sdlc_wizard/order/`
- **Order Summary**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

## Schemas

### Customer Schema
| Column | Type |
|--------|------|
| CustId | String |
| Name | String |
| EmailId | String |
| Region | String |

### Order Schema
| Column | Type |
|--------|------|
| OrderId | String |
| ItemName | String |
| PricePerUnit | Double |
| Qty | Integer |
| Date | String |

### SCD Type 2 Columns (Added)
| Column | Type | Description |
|--------|------|-------------|
| IsActive | Boolean | Current record indicator |
| StartDate | Timestamp | Record validity start |
| EndDate | Timestamp | Record validity end |
| OpTs | Timestamp | Operation timestamp |

## Project Structure
```
glue_pyspark/
├── config/
│   └── glue_params.yaml          # Job configuration
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   └── job.py                # Main job logic
│   └── test/
│       ├── __init__.py
│       └── test_job.py           # Unit tests
├── sample_data/                  # Sample CSV files
│   ├── customerdata.csv
│   └── orderdata.csv
├── .gitignore
├── requirements.txt
└── README.md
```

## Setup

### Prerequisites
- Python 3.9+
- AWS CLI configured
- Access to S3 buckets (s3://adif-sdlc/*)
- AWS Glue permissions

### Installation
```bash
pip install -r requirements.txt
```

## Configuration
Edit `config/glue_params.yaml` to customize:
- S3 paths
- Glue Catalog database name
- Hudi table configurations
- Job parameters

## Running the Job

### Local Testing
```bash
pytest src/test/test_job.py -v
```

### AWS Glue Deployment
```bash
aws glue create-job \
  --name customer-order-analytics-scd2 \
  --role <your-glue-role> \
  --command "Name=glueetl,ScriptLocation=s3://<bucket>/src/main/job.py" \
  --default-arguments '{
    "--job-bookmark-option":"job-bookmark-enable",
    "--enable-metrics":"true",
    "--enable-continuous-cloudwatch-log":"true"
  }'
```

## Features

### Data Quality
- Removes NULL values
- Removes 'Null' string values
- Deduplicates records

### SCD Type 2 Implementation
- Tracks historical changes using Hudi
- Maintains IsActive flag
- Records StartDate/EndDate for versioning
- Uses upsert operation for efficient updates

### Analytics
- Customer aggregate spend calculation
- Order summary generation
- Glue Catalog integration

## Testing
Comprehensive test suite covering:
- Data ingestion (FR-INGEST-001)
- Data cleaning (FR-CLEAN-001)
- SCD Type 2 logic (FR-SCD2-001)
- Schema validation
- Transformation logic
- Hudi integration

Run tests:
```bash
pytest src/test/ -v --cov=src/main
```

## Monitoring
- CloudWatch Logs: `/aws-glue/jobs/output`
- CloudWatch Metrics: Job execution metrics
- Glue Console: Job run history

## Troubleshooting

### Common Issues
1. **S3 Access Denied**: Verify IAM role has GetObject/PutObject permissions
2. **Schema Mismatch**: Ensure CSV headers match defined schemas
3. **Hudi Write Failures**: Check S3 write permissions and Hudi configuration

## License
Proprietary - Internal Use Only