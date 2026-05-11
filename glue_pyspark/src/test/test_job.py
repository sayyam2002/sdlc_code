"""
AWS Glue PySpark Job - Unit Tests
Comprehensive test suite for SDLC Wizard Data Pipeline

Tests all FRD and TRD requirements:
- FR-INGEST-001: Data Ingestion
- FR-CLEAN-001: Data Cleaning
- FR-SCD2-001: SCD Type 2
- FR-AGG-001: Customer Aggregate Spend
- FR-SUMMARY-001: Order Summary
"""

import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType, BooleanType
)
from pyspark.sql.functions import col

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.main.job import SDLCWizardPipeline


@pytest.fixture(scope="session")
def spark():
    """
    Create Spark session for testing
    """
    spark = SparkSession.builder \
        .appName("SDLC-Wizard-Pipeline-Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def pipeline(spark):
    """
    Create pipeline instance for testing
    """
    pipeline = SDLCWizardPipeline(spark.sparkContext)
    pipeline.spark = spark
    pipeline.s3_client = Mock()
    return pipeline


@pytest.fixture
def sample_customer_data(spark):
    """
    Create sample customer data matching TRD schema
    """
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("registration_date", TimestampType(), True),
        StructField("customer_status", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@email.com", "555-0101",
         "123 Main St", "New York", "NY", "10001",
         datetime(2023, 1, 15), "Active"),
        ("C002", "Jane Smith", "jane.smith@email.com", "555-0102",
         "456 Oak Ave", "Los Angeles", "CA", "90001",
         datetime(2023, 2, 20), "Active"),
        ("C003", "Bob Johnson", "bob.johnson@email.com", "555-0103",
         "789 Pine Rd", "Chicago", "IL", "60601",
         datetime(2023, 3, 10), "Active"),
        ("C004", "Alice Williams", "alice.williams@email.com", "555-0104",
         "321 Elm St", "Houston", "TX", "77001",
         datetime(2023, 4, 5), "Inactive"),
        ("C005", "Charlie Brown", "charlie.brown@email.com", "555-0105",
         "654 Maple Dr", "Phoenix", "AZ", "85001",
         datetime(2023, 5, 12), "Active")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """
    Create sample order data matching TRD schema
    """
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), True),
        StructField("order_amount", DecimalType(10, 2), True),
        StructField("order_status", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("payment_method", StringType(), True)
    ])

    data = [
        ("O001", "C001", datetime(2023, 6, 1), Decimal("150.00"),
         "Completed", "P001", 2, "Credit Card"),
        ("O002", "C001", datetime(2023, 6, 15), Decimal("200.00"),
         "Completed", "P002", 1, "PayPal"),
        ("O003", "C002", datetime(2023, 6, 10), Decimal("75.50"),
         "Completed", "P003", 3, "Credit Card"),
        ("O004", "C002", datetime(2023, 7, 5), Decimal("120.00"),
         "Pending", "P001", 1, "Debit Card"),
        ("O005", "C003", datetime(2023, 7, 20), Decimal("300.00"),
         "Completed", "P004", 2, "Credit Card"),
        ("O006", "C003", datetime(2023, 8, 1), Decimal("450.00"),
         "Completed", "P005", 1, "PayPal"),
        ("O007", "C005", datetime(2023, 8, 10), Decimal("99.99"),
         "Completed", "P002", 4, "Credit Card")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """
    Create sample customer data with NULL and 'Null' values for cleaning tests
    """
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("registration_date", TimestampType(), True),
        StructField("customer_status", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@email.com", "555-0101",
         "123 Main St", "New York", "NY", "10001",
         datetime(2023, 1, 15), "Active"),
        (None, "Jane Smith", "jane.smith@email.com", "555-0102",  # NULL customer_id
         "456 Oak Ave", "Los Angeles", "CA", "90001",
         datetime(2023, 2, 20), "Active"),
        ("C003", "Null", "bob.johnson@email.com", "555-0103",  # 'Null' name
         "789 Pine Rd", "Chicago", "IL", "60601",
         datetime(2023, 3, 10), "Active"),
        ("C004", "Alice Williams", None, "555-0104",  # NULL email
         "321 Elm St", "Houston", "TX", "77001",
         datetime(2023, 4, 5), "Inactive"),
        ("C001", "John Doe", "john.doe@email.com", "555-0101",  # Duplicate
         "123 Main St", "New York", "NY", "10001",
         datetime(2023, 1, 15), "Active")
    ]

    return spark.createDataFrame(data, schema)


# Test 1: Initialize Spark Contexts
def test_initialize_spark_contexts(pipeline):
    """
    Test FR-INGEST-001: Spark context initialization
    """
    sc, glue_context, spark, job = pipeline.initialize_spark_contexts()

    assert sc is not None, "SparkContext should be initialized"
    assert glue_context is not None, "GlueContext should be initialized"
    assert spark is not None, "SparkSession should be initialized"
    assert pipeline.s3_client is not None, "S3 client should be initialized"


# Test 2: Validate S3 Access
@patch('boto3.client')
def test_validate_s3_access(mock_boto_client, pipeline):
    """
    Test S3 access validation
    """
    # Mock successful S3 access
    mock_s3 = Mock()
    mock_s3.head_bucket.return_value = {}
    pipeline.s3_client = mock_s3

    result = pipeline.validate_s3_access('s3://adif-sdlc/sdlc_wizard/customerdata/')

    assert result == True, "S3 access validation should succeed"
    mock_s3.head_bucket.assert_called_once()


# Test 3: Read Customer Data
def test_read_customer_data(pipeline, sample_customer_data, tmp_path):
    """
    Test FR-INGEST-001: Reading customer data from S3
    """
    # Write sample data to temp location
    temp_path = str(tmp_path / "customer_data")
    sample_customer_data.write.csv(temp_path, header=True, mode='overwrite')

    # Mock S3 validation
    pipeline.validate_s3_access = Mock(return_value=True)

    # Read data
    df = pipeline.read_data_safe(temp_path, format='csv')

    assert df is not None, "DataFrame should not be None"
    assert df.count() == 5, "Should read 5 customer records"
    assert 'customer_id' in df.columns, "Should have customer_id column"
    assert 'customer_name' in df.columns, "Should have customer_name column"


# Test 4: Read Order Data
def test_read_order_data(pipeline, sample_order_data, tmp_path):
    """
    Test FR-INGEST-001: Reading order data from S3
    """
    # Write sample data to temp location
    temp_path = str(tmp_path / "order_data")
    sample_order_data.write.csv(temp_path, header=True, mode='overwrite')

    # Mock S3 validation
    pipeline.validate_s3_access = Mock(return_value=True)

    # Read data
    df = pipeline.read_data_safe(temp_path, format='csv')

    assert df is not None, "DataFrame should not be None"
    assert df.count() == 7, "Should read 7 order records"
    assert 'order_id' in df.columns, "Should have order_id column"
    assert 'customer_id' in df.columns, "Should have customer_id column"
    assert 'order_amount' in df.columns, "Should have order_amount column"


# Test 5: Clean NULL Values
def test_clean_null_values(pipeline, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001, TR-CLEAN-001: Remove NULL values
    """
    initial_count = sample_customer_data_with_nulls.count()

    cleaned_df = pipeline.clean_data(
        sample_customer_data_with_nulls,
        critical_fields=['customer_id', 'customer_name', 'email']
    )

    final_count = cleaned_df.count()

    assert final_count < initial_count, "Should remove records with NULLs"
    assert final_count == 1, "Should have 1 clean record after removing NULLs and duplicates"

    # Verify no NULLs in critical fields
    null_count = cleaned_df.filter(
        col('customer_id').isNull() |
        col('customer_name').isNull() |
        col('email').isNull()
    ).count()

    assert null_count == 0, "Should have no NULL values in critical fields"


# Test 6: Clean 'Null' String Values
def test_clean_null_strings(pipeline, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001, TR-CLEAN-001: Remove 'Null' string values
    """
    cleaned_df = pipeline.clean_data(
        sample_customer_data_with_nulls,
        critical_fields=['customer_id', 'customer_name', 'email']
    )

    # Verify no 'Null' strings in critical fields
    null_string_count = cleaned_df.filter(
        (col('customer_name') == 'Null') |
        (col('customer_name') == 'NULL') |
        (col('customer_name') == 'null')
    ).count()

    assert null_string_count == 0, "Should have no 'Null' string values"


# Test 7: Remove Duplicates
def test_remove_duplicates(pipeline, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001, TR-CLEAN-001: Remove duplicate records
    """
    cleaned_df = pipeline.clean_data(
        sample_customer_data_with_nulls,
        critical_fields=['customer_id', 'customer_name', 'email']
    )

    # Check for duplicates on primary key
    duplicate_count = cleaned_df.groupBy('customer_id').count().filter(col('count') > 1).count()

    assert duplicate_count == 0, "Should have no duplicate customer_id values"


# Test 8: Add SCD2 Columns
def test_add_scd2_columns(pipeline, sample_customer_data):
    """
    Test FR-SCD2-001, TR-SCD2-001: Add SCD Type 2 columns
    """
    df_with_scd2 = pipeline.add_scd2_columns(sample_customer_data, is_new_record=True)

    # Verify SCD2 columns exist
    assert 'IsActive' in df_with_scd2.columns, "Should have IsActive column"
    assert 'StartDate' in df_with_scd2.columns, "Should have StartDate column"
    assert 'EndDate' in df_with_scd2.columns, "Should have EndDate column"
    assert 'OpTs' in df_with_scd2.columns, "Should have OpTs column"

    # Verify default values for new records
    first_row = df_with_scd2.first()
    assert first_row['IsActive'] == True, "New records should have IsActive=True"
    assert first_row['EndDate'] is not None, "Should have EndDate set"


# Test 9: SCD2 Upsert - New Records
def test_scd2_upsert_new_records(pipeline, sample_customer_data):
    """
    Test FR-SCD2-001, TR-SCD2-001: SCD Type 2 upsert for new records
    """
    result_df = pipeline.perform_scd2_upsert(
        new_df=sample_customer_data,
        existing_df=None,
        primary_key='customer_id',
        compare_columns=['customer_name', 'email', 'phone']
    )

    assert result_df.count() == sample_customer_data.count(), \
        "All records should be inserted as new"

    # Verify all records are active
    active_count = result_df.filter(col('IsActive') == True).count()
    assert active_count == result_df.count(), "All new records should be active"


# Test 10: SCD2 Upsert - Changed Records
def test_scd2_upsert_changed_records(pipeline, sample_customer_data, spark):
    """
    Test FR-SCD2-001, TR-SCD2-001: SCD Type 2 upsert for changed records
    """
    # Create existing data with SCD2 columns
    existing_df = pipeline.add_scd2_columns(sample_customer_data, is_new_record=True)

    # Create changed data (update email for C001)
    changed_data = sample_customer_data.filter(col('customer_id') == 'C001') \
        .withColumn('email', lit('john.doe.new@email.com'))

    result_df = pipeline.perform_scd2_upsert(
        new_df=changed_data,
        existing_df=existing_df,
        primary_key='customer_id',
        compare_columns=['customer_name', 'email', 'phone']
    )

    # Should have 2 records for C001 (expired old + new active)
    c001_records = result_df.filter(col('customer_id') == 'C001').count()
    assert c001_records == 2, "Should have 2 records for changed customer (old + new)"

    # Verify one is active, one is inactive
    c001_active = result_df.filter(
        (col('customer_id') == 'C001') & (col('IsActive') == True)
    ).count()
    assert c001_active == 1, "Should have 1 active record for C001"


# Test 11: Calculate Customer Aggregate Spend
def test_calculate_customer_aggregate_spend(pipeline, sample_customer_data, sample_order_data):
    """
    Test FR-AGG-001, TR-AGG-001: Calculate customer aggregate spend
    """
    result_df = pipeline.calculate_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data
    )

    assert result_df is not None, "Result should not be None"
    assert result_df.count() > 0, "Should have aggregate results"

    # Verify columns
    assert 'customer_id' in result_df.columns, "Should have customer_id"
    assert 'customer_name' in result_df.columns, "Should have customer_name"
    assert 'total_spend' in result_df.columns, "Should have total_spend"
    assert 'order_count' in result_df.columns, "Should have order_count"
    assert 'avg_spend_per_order' in result_df.columns, "Should have avg_spend_per_order"

    # Verify calculations for C001 (2 orders: 150 + 200 = 350)
    c001_spend = result_df.filter(col('customer_id') == 'C001').first()
    assert c001_spend is not None, "Should have spend data for C001"
    assert float(c001_spend['total_spend']) == 350.0, "C001 total spend should be 350"
    assert c001_spend['order_count'] == 2, "C001 should have 2 orders"


# Test 12: Generate Order Summary
def test_generate_order_summary(pipeline, sample_order_data):
    """
    Test FR-SUMMARY-001: Generate order summary
    """
    result_df = pipeline.generate_order_summary(sample_order_data)

    assert result_df is not None, "Result should not be None"
    assert result_df.count() > 0, "Should have summary results"

    # Verify columns
    assert 'customer_id' in result_df.columns, "Should have customer_id"
    assert 'total_orders' in result_df.columns, "Should have total_orders"
    assert 'total_amount' in result_df.columns, "Should have total_amount"
    assert 'avg_order_amount' in result_df.columns, "Should have avg_order_amount"
    assert 'first_order_date' in result_df.columns, "Should have first_order_date"
    assert 'last_order_date' in result_df.columns, "Should have last_order_date"

    # Verify calculations for C001
    c001_summary = result_df.filter(col('customer_id') == 'C001').first()
    assert c001_summary is not None, "Should have summary for C001"
    assert c001_summary['total_orders'] == 2, "C001 should have 2 orders"


# Test 13: Write Data Safe
def test_write_data_safe(pipeline, sample_customer_data, tmp_path):
    """
    Test safe data writing to S3
    """
    output_path = str(tmp_path / "output")

    # Mock S3 validation
    pipeline.validate_s3_access = Mock(return_value=True)

    result = pipeline.write_data_safe(
        sample_customer_data,
        output_path,
        format='parquet',
        mode='overwrite'
    )

    assert result == True, "Write should succeed"

    # Verify data was written
    written_df = pipeline.spark.read.parquet(output_path)
    assert written_df.count() == sample_customer_data.count(), \
        "Written data should match source data count"


# Test 14: End-to-End Pipeline Test
def test_end_to_end_pipeline(pipeline, sample_customer_data, sample_order_data, tmp_path):
    """
    Test complete pipeline execution
    Tests all FRD requirements: FR-INGEST-001, FR-CLEAN-001, FR-SCD2-001,
    FR-AGG-001, FR-SUMMARY-001
    """
    # Setup temp paths
    customer_input = str(tmp_path / "customer_input")
    order_input = str(tmp_path / "order_input")
    customer_output = str(tmp_path / "customer_output")
    order_output = str(tmp_path / "order_output")
    spend_output = str(tmp_path / "spend_output")
    summary_output = str(tmp_path / "summary_output")

    # Write sample data
    sample_customer_data.write.csv(customer_input, header=True, mode='overwrite')
    sample_order_data.write.csv(order_input, header=True, mode='overwrite')

    # Update pipeline config
    pipeline.config['source_paths']['customer_data'] = customer_input
    pipeline.config['source_paths']['order_data'] = order_input
    pipeline.config['output_paths']['customer_catalog'] = customer_output
    pipeline.config['output_paths']['order_catalog'] = order_output
    pipeline.config['output_paths']['customer_aggregate_spend'] = spend_output
    pipeline.config['output_paths']['order_summary'] = summary_output

    # Mock S3 validation
    pipeline.validate_s3_access = Mock(return_value=True)

    # Mock Hudi write (use parquet instead for testing)
    original_write = pipeline.write_data_safe
    def mock_write(df, path, format='parquet', **kwargs):
        if format == 'hudi':
            format = 'parquet'
        return original_write(df, path, format=format, mode='overwrite')

    pipeline.write_data_safe = mock_write

    # Run pipeline
    result = pipeline.run_pipeline()

    assert result == True, "Pipeline should complete successfully"

    # Verify outputs exist
    customer_df = pipeline.spark.read.parquet(customer_output)
    assert customer_df.count() > 0, "Customer output should have data"
    assert 'IsActive' in customer_df.columns, "Customer output should have SCD2 columns"

    order_df = pipeline.spark.read.parquet(order_output)
    assert order_df.count() > 0, "Order output should have data"

    spend_df = pipeline.spark.read.parquet(spend_output)
    assert spend_df.count() > 0, "Spend output should have data"
    assert 'total_spend' in spend_df.columns, "Spend output should have aggregations"

    summary_df = pipeline.spark.read.parquet(summary_output)
    assert summary_df.count() > 0, "Summary output should have data"
    assert 'total_orders' in summary_df.columns, "Summary output should have metrics"


# Test 15: Error Handling
def test_error_handling(pipeline):
    """
    Test error handling for invalid S3 paths and data
    """
    # Test invalid S3 path
    pipeline.validate_s3_access = Mock(return_value=False)

    result = pipeline.read_data_safe('s3://invalid-bucket/invalid-path/')
    assert result is None, "Should return None for invalid S3 path"

    # Test write to invalid path
    from pyspark.sql.types import StructType, StructField, StringType
    empty_df = pipeline.spark.createDataFrame([], StructType([
        StructField("test", StringType(), True)
    ]))

    result = pipeline.write_data_safe(empty_df, 's3://invalid-bucket/invalid-path/')
    assert result == False, "Should return False for invalid write path"


# Test 16: Schema Validation
def test_get_customer_schema(pipeline):
    """
    Test customer schema matches TRD requirements
    """
    schema = pipeline.get_customer_schema()

    assert schema is not None, "Schema should not be None"

    field_names = [field.name for field in schema.fields]

    # Verify all required fields from TRD
    required_fields = [
        'customer_id', 'customer_name', 'email', 'phone',
        'address', 'city', 'state', 'zip_code',
        'registration_date', 'customer_status'
    ]

    for field in required_fields:
        assert field in field_names, f"Schema should have {field} field"


# Test 17: Schema Validation - Order
def test_get_order_schema(pipeline):
    """
    Test order schema matches TRD requirements
    """
    schema = pipeline.get_order_schema()

    assert schema is not None, "Schema should not be None"

    field_names = [field.name for field in schema.fields]

    # Verify all required fields from TRD
    required_fields = [
        'order_id', 'customer_id', 'order_date', 'order_amount',
        'order_status', 'product_id', 'quantity', 'payment_method'
    ]

    for field in required_fields:
        assert field in field_names, f"Schema should have {field} field"


# Test 18: Configuration Validation
def test_configuration_validation(pipeline):
    """
    Test pipeline configuration matches TRD requirements
    """
    config = pipeline.config

    # Verify S3 paths from TRD
    assert config['source_paths']['customer_data'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'
    assert config['source_paths']['order_data'] == 's3://adif-sdlc/sdlc_wizard/orderdata/'
    assert config['output_paths']['customer_catalog'] == 's3://adif-sdlc/catalog/sdlc_wizard_customer/'
    assert config['output_paths']['order_catalog'] == 's3://adif-sdlc/catalog/sdlc_wizard_order/'
    assert config['output_paths']['order_summary'] == 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
    assert config['output_paths']['customer_aggregate_spend'] == 's3://adif-sdlc/analytics/customeraggregatespend/'

    # Verify Glue database
    assert config['glue_database'] == 'sdlc_wizard'

    # Verify SCD2 columns
    assert config['scd2']['is_active'] == 'IsActive'
    assert config['scd2']['start_date'] == 'StartDate'
    assert config['scd2']['end_date'] == 'EndDate'
    assert config['scd2']['op_ts'] == 'OpTs'


# Test 19: Data Quality - Customer
def test_data_quality_customer(pipeline, sample_customer_data):
    """
    Test data quality checks for customer data
    """
    # All customer_ids should be non-null
    null_count = sample_customer_data.filter(col('customer_id').isNull()).count()
    assert null_count == 0, "Customer data should have no NULL customer_ids"

    # All emails should contain @
    invalid_emails = sample_customer_data.filter(
        ~col('email').contains('@')
    ).count()
    assert invalid_emails == 0, "All emails should be valid format"


# Test 20: Data Quality - Order
def test_data_quality_order(pipeline, sample_order_data):
    """
    Test data quality checks for order data
    """
    # All order amounts should be positive
    negative_amounts = sample_order_data.filter(col('order_amount') <= 0).count()
    assert negative_amounts == 0, "All order amounts should be positive"

    # All order_ids should be non-null
    null_orders = sample_order_data.filter(col('order_id').isNull()).count()
    assert null_orders == 0, "Order data should have no NULL order_ids"

    # All orders should have valid customer_ids
    null_customers = sample_order_data.filter(col('customer_id').isNull()).count()
    assert null_customers == 0, "All orders should have customer_ids"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])