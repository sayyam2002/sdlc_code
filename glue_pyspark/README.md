# Customer Order Analytics - AWS Glue PySpark ETL Job

## Overview
This AWS Glue PySpark job implements a comprehensive ETL pipeline for customer order analytics with SCD Type 2 historical tracking using Apache Hudi format.

## Functional Requirements (FRD)

### FR-INGEST-001: Data Ingestion
- Ingest customer data from `s3://adif-sdlc/sdlc_wizard/customerdata/`
- Ingest order data from `s3://adif-sdlc/sdlc_wizard/orderdata/`

### FR-CLEAN-001: Data Cleaning
- Remove NULL values from all columns
- Remove rows containing string literal "Null" in any column
- Remove duplicate records across both datasets

### FR-JOIN-001: Data Integration
- Join customer and order datasets on CustId
- Produce enriched order summary dataset

### FR-AGG-001: Aggregation
- Calculate total spend per customer
- Output to `s3://adif-sdlc/analytics/customeraggregatespend/`

### FR-SCD2-001: Historical Tracking
- Implement SCD Type 2 for order summary
- Add columns: IsActive, StartDate, EndDate, OpTs
- Use Apache Hudi format with upsert operation
- Output to `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`

## Technical Requirements (TRD)

### Data Sources
1. **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
   - Schema: CustId (string), Name (string), EmailId (string), Region (string)
   - Format: Parquet
   - Encoding: UTF-8

2. **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`
   - Schema: CustId (string), Name (string), EmailId (string), Region (string), OrderId (string), ItemName (string), PricePerUnit (double), Qty (int), Date (string), IsActive (boolean)
   - Format: Parquet
   - Storage: S3

### Data Outputs
1. **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`
   - Format: Parquet
   - Partitioned by: Region

2. **Order Summary (SCD Type 2)**: `s3://adif-sdlc/curated/sdlc_wizard/ordersummary/`
   - Format: Apache Hudi
   - Record Key: OrderId
   - Precombine Field: OpTs
   - Operation: Upsert

### Transformations
1. **Data Cleaning**:
   - Filter out NULL values
   - Filter out "Null" string literals
   - Remove duplicates

2. **Business Logic**:
   - Calculate TotalAmount = PricePerUnit * Qty
   - Aggregate total spend per customer by Region

3. **SCD Type 2**:
   - IsActive: Current record indicator (boolean)
   - StartDate: Record effective start timestamp
   - EndDate: Record effective end timestamp (null for current)
   - OpTs: Operation timestamp for versioning

## Project Structure
```
glue_pyspark/
├── config/
│   └── glue_params.yaml          # Job parameters configuration
├── src/
│   ├── __init__.py
│   ├── main/
│   │   ├── __init__.py
│   │   └── job.py                # Main ETL logic
│   └── test/
│       ├── __init__.py
│       └── test_job.py           # Comprehensive test suite
├── .gitignore
├── README.md
└── requirements.txt
```

## Configuration
All job parameters are defined in `config/glue_params.yaml`:
- S3 input/output paths
- Hudi configuration
- Glue Catalog settings
- Job-specific parameters

## Installation

### Prerequisites
- Python 3.7+
- AWS Glue 3.0+ (PySpark 3.1+)
- Apache Hudi 0.10.0+

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest src/test/test_job.py -v --cov=src/main --cov-report=html
```

## Usage

### AWS Glue Console
1. Upload code to S3: `s3://your-scripts-bucket/glue_pyspark/`
2. Create Glue Job with:
   - Type: Spark
   - Glue Version: 3.0
   - Language: Python 3
   - Script location: `s3://your-scripts-bucket/glue_pyspark/src/main/job.py`
   - Additional files: `s3://your-scripts-bucket/glue_pyspark/config/glue_params.yaml`

### AWS CLI
```bash
aws glue start-job-run \
  --job-name customer-order-analytics \
  --arguments '--config_path=s3://your-scripts-bucket/glue_pyspark/config/glue_params.yaml'
```

### Programmatic Execution
```python
from src.main.job import main
main()
```

## Testing
The test suite covers:
- FR-INGEST-001: Data ingestion validation
- FR-CLEAN-001: Data cleaning logic
- FR-JOIN-001: Join operations
- FR-AGG-001: Aggregation calculations
- FR-SCD2-001: SCD Type 2 implementation
- Schema validation
- S3 path validation
- Hudi configuration validation
- Error handling

Run tests:
```bash
pytest src/test/test_job.py -v
```

## Monitoring
- CloudWatch Logs: `/aws-glue/jobs/output`
- CloudWatch Metrics: Job duration, data processed, errors
- Glue Data Catalog: Table metadata and statistics

## Troubleshooting

### Common Issues
1. **S3 Access Denied**: Verify IAM role has S3 read/write permissions
2. **Hudi Write Failures**: Check Hudi configuration and S3 path permissions
3. **Schema Mismatch**: Validate input data schemas match TRD specifications
4. **Memory Issues**: Increase DPU allocation in Glue job configuration

## Performance Optimization
- Partition output data by Region
- Use Hudi's upsert for efficient SCD Type 2 updates
- Enable Glue job bookmarks for incremental processing
- Optimize Spark configurations based on data volume

## Security
- All S3 paths use encryption at rest (SSE-S3)
- IAM roles follow least privilege principle
- No hardcoded credentials in code
- Sensitive parameters stored in AWS Secrets Manager (if applicable)

## Maintenance
- Review and update schemas as business requirements evolve
- Monitor job execution metrics and optimize as needed
- Update Hudi version for performance improvements
- Regular testing with production-like data volumes

## Contact
For issues or questions, contact the Data Engineering team.

## License
Proprietary - Internal Use Only