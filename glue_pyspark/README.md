# AWS Glue PySpark Job - Customer Order Analytics with SCD Type 2

## Project Overview
This AWS Glue PySpark job implements customer order data ingestion, cleaning, aggregation, and SCD Type 2 historical tracking using Apache Hudi format.

## Functional Requirements Implemented

### FR-INGEST-001: Customer Data Ingestion
- Source: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- Schema: CustId (string), Name (string), EmailId (string), Region (string)
- Format: CSV with UTF-8 encoding, comma delimiter, header row

### FR-INGEST-002: Order Data Ingestion
- Source: `s3://adif-sdlc/sdlc_wizard/orderdata/`
- Schema: OrderId (string), ItemName (string), PricePerUnit (decimal), Qty (integer), Date (string)
- Format: CSV with UTF-8 encoding, comma delimiter, header row

### FR-CLEAN-001: Data Cleaning
- Remove NULL values
- Remove 'Null' string values
- Remove duplicate records

### FR-SCD2-001: SCD Type 2 Implementation
- Track historical changes in customer data
- Columns: IsActive (boolean), StartDate (timestamp), EndDate (timestamp), OpTs (timestamp)
- Storage: Apache Hudi format with upsert operation
- Output: `s3://adif-sdlc/sdlc_wizard/customerdata/` (Hudi format)

### FR-AGG-001: Customer Aggregate Spend
- Calculate total spend per customer
- Output: `s3://adif-sdlc/analytics/customeraggregatespend/`

### FR-SUMMARY-001: Order Summary
- Generate order summary with customer details
- Output: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`

## Technical Architecture

### AWS Services
- **AWS Glue**: ETL job orchestration
- **Amazon S3**: Data lake storage
- **AWS Glue Data Catalog**: Metadata management
- **Apache Hudi**: SCD Type 2 implementation

### Data Flow
```
Customer Data (S3) ──┐
                     ├──> Cleaning ──> SCD Type 2 (Hudi) ──┐
Order Data (S3) ─────┘                                      ├──> Aggregations ──> Analytics (S3)
                                                            └──> Order Summary ──> Curated (S3)
```

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
    ├── customer_data.csv
    └── order_data.csv
```

## Setup Instructions

### Prerequisites
- Python 3.9+
- AWS CLI configured
- AWS Glue permissions
- S3 bucket access: `s3://adif-sdlc/`

### Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Set AWS credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### Configuration
Edit `config/glue_params.yaml` to customize:
- S3 paths
- Glue database name
- Hudi configuration
- Job parameters

## Running the Job

### Local Testing
```bash
# Run tests
pytest src/test/test_job.py -v

# Run with coverage
pytest src/test/test_job.py --cov=src/main --cov-report=html
```

### AWS Glue Deployment
```bash
# Upload job script to S3
aws s3 cp src/main/job.py s3://adif-sdlc/glue-scripts/

# Create Glue job
aws glue create-job \
  --name customer-order-analytics-scd2 \
  --role AWSGlueServiceRole \
  --command "Name=glueetl,ScriptLocation=s3://adif-sdlc/glue-scripts/job.py" \
  --default-arguments '{
    "--job-bookmark-option":"job-bookmark-enable",
    "--enable-metrics":"true",
    "--enable-continuous-cloudwatch-log":"true"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X"

# Run Glue job
aws glue start-job-run --job-name customer-order-analytics-scd2
```

## Data Schemas

### Customer Data (Input)
| Column   | Type   | Description          |
|----------|--------|----------------------|
| CustId   | string | Customer ID (PK)     |
| Name     | string | Customer name        |
| EmailId  | string | Email address        |
| Region   | string | Geographic region    |

### Order Data (Input)
| Column       | Type    | Description           |
|--------------|---------|------------------------|
| OrderId      | string  | Order ID (PK)         |
| ItemName     | string  | Product name          |
| PricePerUnit | decimal | Unit price            |
| Qty          | integer | Quantity ordered      |
| Date         | string  | Order date            |

### Customer Data with SCD Type 2 (Output - Hudi)
| Column    | Type      | Description                    |
|-----------|-----------|--------------------------------|
| CustId    | string    | Customer ID (Record Key)       |
| Name      | string    | Customer name                  |
| EmailId   | string    | Email address                  |
| Region    | string    | Geographic region              |
| IsActive  | boolean   | Current record flag            |
| StartDate | timestamp | Record effective start date    |
| EndDate   | timestamp | Record effective end date      |
| OpTs      | timestamp | Operation timestamp            |

### Customer Aggregate Spend (Output)
| Column      | Type    | Description           |
|-------------|---------|------------------------|
| CustId      | string  | Customer ID           |
| Name        | string  | Customer name         |
| TotalSpend  | decimal | Total amount spent    |

### Order Summary (Output)
| Column       | Type    | Description           |
|--------------|---------|------------------------|
| OrderId      | string  | Order ID              |
| CustId       | string  | Customer ID           |
| Name         | string  | Customer name         |
| ItemName     | string  | Product name          |
| TotalAmount  | decimal | Order total amount    |
| Date         | string  | Order date            |

## SCD Type 2 Implementation Details

### Hudi Configuration
- **Table Type**: Copy on Write (COW)
- **Record Key**: CustId
- **Precombine Field**: OpTs
- **Operation**: UPSERT
- **Partition**: None (can be added by Region if needed)

### Change Detection Logic
1. Read existing Hudi table (if exists)
2. Compare incoming records with existing records
3. For changed records:
   - Set IsActive=False, EndDate=current_timestamp for old record
   - Insert new record with IsActive=True, StartDate=current_timestamp
4. For new records:
   - Insert with IsActive=True, StartDate=current_timestamp, EndDate=null

## Monitoring and Logging

### CloudWatch Logs
- Job execution logs: `/aws-glue/jobs/logs-v2/`
- Error logs: `/aws-glue/jobs/error/`
- Output logs: `/aws-glue/jobs/output/`

### Metrics
- Records processed
- Data quality metrics
- Job duration
- DPU consumption

## Troubleshooting

### Common Issues

**Issue**: S3 Access Denied
```
Solution: Verify IAM role has s3:GetObject, s3:PutObject permissions
```

**Issue**: Hudi write failure
```
Solution: Check Hudi configuration, ensure sufficient memory allocation
```

**Issue**: Schema mismatch
```
Solution: Verify CSV headers match expected schema
```

## Performance Optimization

### Best Practices
1. **Partitioning**: Consider partitioning by Region for large datasets
2. **Caching**: Cache frequently accessed DataFrames
3. **Broadcast Joins**: Use broadcast for small dimension tables
4. **Coalesce**: Reduce output file count with coalesce()
5. **Predicate Pushdown**: Filter data early in the pipeline

### Resource Allocation
- **Small datasets (<1GB)**: 2-5 DPUs
- **Medium datasets (1-10GB)**: 5-10 DPUs
- **Large datasets (>10GB)**: 10-50 DPUs

## Testing Strategy

### Unit Tests
- Data ingestion validation
- Schema validation
- Data cleaning logic
- SCD Type 2 logic
- Aggregation calculations

### Integration Tests
- End-to-end pipeline execution
- S3 read/write operations
- Glue Catalog integration
- Hudi upsert operations

### Test Coverage
- Minimum 80% code coverage
- All FRD requirements tested
- Edge cases covered

## Security

### Data Encryption
- **At Rest**: S3 SSE-S3 encryption
- **In Transit**: TLS 1.2+

### Access Control
- IAM roles with least privilege
- S3 bucket policies
- Glue resource policies

## Maintenance

### Regular Tasks
- Monitor job execution metrics
- Review CloudWatch logs
- Optimize Hudi compaction
- Update dependencies
- Review and update test cases

## Support
For issues or questions, contact the data engineering team.

## License
Internal use only - Proprietary