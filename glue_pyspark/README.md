# AWS Glue PySpark ETL Job - Customer and Order Data Processing

## Overview
This AWS Glue PySpark job ingests customer and order data from S3, performs data quality transformations, writes cleaned data to S3 in Parquet format, registers tables in AWS Glue Data Catalog, and calculates customer aggregate spending.

## Functional Requirements
- **FR-INGEST-001**: Load Customer CSV Data from S3
  - Read customer data from S3 CSV files
  - Validate schema and data quality
  - Enable downstream processing

## Technical Implementation

### Data Sources
- **Customer Data**: `s3://adif-sdlc/sdlc_wizard/customerdata/`
- **Order Data**: `s3://adif-sdlc/sdlc_wizard/orderdata/`

### Data Targets
- **Cleaned Customer Data**: `s3://<BUCKET_NAME>/catalog/sdlc_wizard/customer/`
- **Cleaned Order Data**: `s3://<BUCKET_NAME>/catalog/sdlc_wizard/order/`
- **Customer Aggregate Spend**: `s3://adif-sdlc/analytics/customeraggregatespend/`

### Glue Catalog
- **Database**: `gen_ai_poc_databrickscoe`
- **Tables**: `customer`, `order`, `customeraggregatespend`

### Schemas

#### Customer Schema
| Column Name | Data Type | Nullable | Notes |
|-------------|-----------|----------|-------|
| custid | String | Yes | Primary key candidate |
| name | String | Yes | Customer name |
| emailid | String | Yes | Email address |
| region | String | Yes | Geographic region |

#### Order Schema
| Column Name | Data Type | Nullable | Notes |
|-------------|-----------|----------|-------|
| orderid | String | Yes | Primary key |
| itemname | String | Yes | Item name |
| priceperunit | String | Yes | Price per unit |
| qty | String | Yes | Quantity |
| date | String | Yes | Order date |
| custid | String | Yes | Foreign key to customer |

### Transformations
1. **Data Ingestion**: Read CSV files from S3
2. **Column Normalization**: Convert all column names to lowercase
3. **Data Cleaning**:
   - Remove records with NULL values
   - Remove records with string literal "Null"
   - Remove duplicate records
4. **Catalog Registration**: Write to Parquet and register in Glue Catalog
5. **Aggregation**: Calculate total spending per customer per date

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
- Source paths for customer and order data
- Target paths for cleaned data and aggregations
- Glue catalog database and table names
- File formats and options

## Running the Job

### AWS Glue Console
1. Upload the job script to S3
2. Create a new Glue job pointing to the script
3. Configure job parameters from `glue_params.yaml`
4. Run the job

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest src/test/test_job.py -v
```

## Error Handling
- Source path validation with detailed error messages
- CSV parsing failure detection
- Schema validation against expected columns
- Comprehensive logging at INFO, WARNING, and ERROR levels

## Dependencies
- PySpark (provided by AWS Glue)
- PyYAML for configuration management
- boto3 for AWS SDK operations

## Sample Data
Sample CSV files are provided in `sample_data/` directory for testing purposes.

## Notes
- All column names are normalized to lowercase for consistency
- Duplicate removal retains the first occurrence
- Empty files are valid but logged as warnings
- All S3 paths are validated before operations