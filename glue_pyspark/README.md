# AWS Glue PySpark ETL Job - Customer Order Analytics with SCD Type 2

## Project Overview
This AWS Glue ETL job implements a complete data pipeline for customer and order data processing with SCD Type 2 historical tracking using Apache Hudi format.

## Features
- **Data Ingestion**: Reads customer and order data from S3 curated zones
- **Data Cleaning**: Removes NULL values, 'Null' strings, and duplicates
- **SCD Type 2**: Tracks historical changes using Hudi format with IsActive, StartDate, EndDate, OpTs columns
- **Aggregation**: Calculates customer aggregate spend metrics
- **AWS Glue Integration**: Full Glue Catalog integration with database registration

## Architecture

### Data Flow
```
S3 Curated Zone (Customer/Order)
    ↓
Data Ingestion & Validation
    ↓
Data Cleaning (NULL removal, deduplication)
    ↓
SCD Type 2 Processing (Hudi Upsert)
    ↓
Aggregation (Customer Spend Analytics)
    ↓
S3 Analytics Zone + Glue Catalog Registration
```

### S3 Paths (from TRD)
- **Customer Source**: `s3://adif-sdlc/curated/sdlc_wizard/customer/`
- **Order Source**: `s3://adif-sdlc/curated/sdlc_wizard/order/`
- **Order Summary Output**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
- **Customer Aggregate Output**: `s3://adif-sdlc/analytics/customeraggregatespend/`
- **Athena Results**: `s3://adif-sdlc/athena-results/`

## Technical Stack
- **Runtime**: AWS Glue 4.0 (Spark 3.3.0, Python 3.10)
- **Storage Format**: Apache Hudi (SCD Type 2)
- **Catalog**: AWS Glue Data Catalog
- **Testing**: pytest with moto for AWS mocking

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

## Installation

### Prerequisites
- Python 3.10+
- AWS CLI configured
- Access to S3 bucket: `s3://adif-sdlc/`
- AWS Glue permissions

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials
aws configure
```

## Configuration

### glue_params.yaml
```yaml
source_paths:
  customer: "s3://adif-sdlc/curated/sdlc_wizard/customer/"
  order: "s3://adif-sdlc/curated/sdlc_wizard/order/"

target_paths:
  order_summary: "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
  customer_aggregate: "s3://adif-sdlc/analytics/customeraggregatespend/"

glue_catalog:
  database: "sdlc_wizard_db"

hudi_config:
  table_type: "COPY_ON_WRITE"
  operation: "upsert"
```

## Usage

### Running Locally (Development)
```bash
# Run the job
python src/main/job.py

# Run tests
pytest src/test/test_job.py -v --cov=src/main --cov-report=html
```

### Deploying to AWS Glue
```bash
# Upload to S3
aws s3 cp src/main/job.py s3://adif-sdlc/scripts/glue/customer_order_etl.py
aws s3 cp config/glue_params.yaml s3://adif-sdlc/config/glue_params.yaml

# Create Glue Job
aws glue create-job \
  --name customer-order-scd2-etl \
  --role AWSGlueServiceRole \
  --command "Name=glueetl,ScriptLocation=s3://adif-sdlc/scripts/glue/customer_order_etl.py,PythonVersion=3" \
  --default-arguments '{
    "--job-language":"python",
    "--enable-glue-datacatalog":"true",
    "--enable-continuous-cloudwatch-log":"true",
    "--config_path":"s3://adif-sdlc/config/glue_params.yaml"
  }' \
  --glue-version "4.0" \
  --number-of-workers 10 \
  --worker-type G.1X
```

### Running on AWS Glue
```bash
aws glue start-job-run --job-name customer-order-scd2-etl
```

## Data Schemas

### Customer Schema (Input)
```
customer_id: string
customer_name: string
email: string
phone: string
address: string
city: string
state: string
zip_code: string
registration_date: timestamp
```

### Order Schema (Input)
```
order_id: string
customer_id: string
order_date: timestamp
order_amount: decimal(10,2)
order_status: string
product_id: string
quantity: integer
unit_price: decimal(10,2)
```

### SCD Type 2 Schema (Output)
All input columns plus:
```
IsActive: boolean
StartDate: timestamp
EndDate: timestamp
OpTs: timestamp
```

### Customer Aggregate Spend Schema (Output)
```
customer_id: string
customer_name: string
total_orders: long
total_spend: decimal(20,2)
avg_order_value: decimal(20,2)
first_order_date: timestamp
last_order_date: timestamp
IsActive: boolean
StartDate: timestamp
EndDate: timestamp
OpTs: timestamp
```

## SCD Type 2 Implementation

### Hudi Configuration
- **Record Key**: customer_id (for customer), order_id (for order)
- **Precombine Field**: OpTs
- **Operation**: UPSERT
- **Table Type**: COPY_ON_WRITE

### Change Tracking Logic
1. New records: IsActive=True, StartDate=current_timestamp, EndDate=null
2. Updated records:
   - Old version: IsActive=False, EndDate=current_timestamp
   - New version: IsActive=True, StartDate=current_timestamp, EndDate=null
3. Unchanged records: Maintain existing SCD fields

## Data Quality Rules

### Cleaning Operations
1. **NULL Removal**: Remove rows where key fields are NULL
2. **'Null' String Removal**: Remove rows with string 'Null' values
3. **Deduplication**: Keep latest record based on timestamp
4. **Validation**: Ensure required fields are present

### Key Fields (Cannot be NULL)
- Customer: customer_id, customer_name, email
- Order: order_id, customer_id, order_date, order_amount

## Testing

### Test Coverage
- ✅ Spark context initialization
- ✅ S3 read/write operations
- ✅ Data cleaning (NULL, 'Null', duplicates)
- ✅ SCD Type 2 column addition
- ✅ Hudi configuration validation
- ✅ Customer aggregation logic
- ✅ Schema validation
- ✅ Glue Catalog integration
- ✅ Error handling

### Running Tests
```bash
# Run all tests
pytest src/test/test_job.py -v

# Run with coverage
pytest src/test/test_job.py --cov=src/main --cov-report=term-missing

# Run specific test
pytest src/test/test_job.py::test_scd2_columns -v
```

## Monitoring

### CloudWatch Metrics
- Job duration
- Records processed
- Data quality metrics
- Error rates

### Logging
- INFO: Job progress and statistics
- WARNING: Data quality issues
- ERROR: Job failures

## Troubleshooting

### Common Issues

**Issue**: S3 Access Denied
```
Solution: Verify IAM role has s3:GetObject and s3:PutObject permissions
```

**Issue**: Hudi Write Failure
```
Solution: Check Hudi configuration and ensure sufficient disk space
```

**Issue**: Schema Mismatch
```
Solution: Verify input data matches expected schema in TRD
```

## Performance Optimization

### Recommendations
1. **Partitioning**: Partition by date for time-series data
2. **Caching**: Cache frequently accessed DataFrames
3. **Broadcast Joins**: Use for small dimension tables
4. **Worker Sizing**: Use G.1X or G.2X workers based on data volume

### Glue Job Parameters
```
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
```

## Security

### IAM Permissions Required
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

## Maintenance

### Regular Tasks
- Monitor job execution logs
- Review data quality metrics
- Update schemas as business requirements change
- Optimize Hudi compaction settings

## Support
For issues or questions, contact the Data Engineering team.

## License
Internal use only - ADIF SDLC Project