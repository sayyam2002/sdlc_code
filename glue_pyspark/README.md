# AWS Glue PySpark Job - Minimal Template

## Overview
This is a minimal AWS Glue PySpark job template created because the source FRD/TRD contains **NO REQUIREMENTS IDENTIFIED**.

**Status**: ⚠️ Template Only - Requires Actual Requirements

## Project Structure
```
glue_pyspark/
├── config/
│   └── glue_params.yaml          # Job parameters
├── src/
│   ├── main/
│   │   ├── __init__.py
│   │   └── job.py                # Main Glue job
│   └── test/
│       ├── __init__.py
│       └── test_job.py           # Unit tests
├── sample_data/
│   └── sample_input.csv          # Sample data for testing
├── .gitignore
├── README.md
├── requirements.txt
└── pytest.ini
```

## Requirements
- Python 3.9+
- PySpark 3.3+
- AWS Glue 4.0
- pytest 7.0+

## Installation
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration
Edit `config/glue_params.yaml` with your specific parameters:
- S3 paths
- Database names
- Table names
- Processing parameters

## Running Locally
```bash
# Run the job
python src/main/job.py

# Run tests
pytest src/test/ -v
```

## Deploying to AWS Glue
```bash
# Upload script to S3
aws s3 cp src/main/job.py s3://your-bucket/scripts/

# Upload config to S3
aws s3 cp config/glue_params.yaml s3://your-bucket/config/

# Create Glue job (example)
aws glue create-job \
  --name "minimal-glue-job" \
  --role "AWSGlueServiceRole" \
  --command "Name=glueetl,ScriptLocation=s3://your-bucket/scripts/job.py,PythonVersion=3" \
  --default-arguments '{
    "--config_path":"s3://your-bucket/config/glue_params.yaml",
    "--enable-metrics":"true",
    "--enable-spark-ui":"true",
    "--enable-job-insights":"true"
  }' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X"
```

## Testing
```bash
# Run all tests
pytest src/test/ -v --cov=src/main

# Run specific test
pytest src/test/test_job.py::test_main_execution -v
```

## Next Steps
1. **Add Actual Requirements**: Update this template with real FRD/TRD requirements
2. **Define Schemas**: Add input/output data schemas
3. **Implement Transformations**: Add business logic
4. **Configure S3 Paths**: Set actual S3 bucket paths
5. **Add Data Quality Checks**: Implement validation rules
6. **Enhance Testing**: Add comprehensive test cases

## Notes
- This template uses AWS Glue pre-initialized contexts (spark, glueContext, sc)
- No SparkContext/SparkSession creation in main code
- Sample data included for local testing
- Pytest fixtures handle context creation for tests

## Support
For issues or questions, refer to AWS Glue documentation:
https://docs.aws.amazon.com/glue/