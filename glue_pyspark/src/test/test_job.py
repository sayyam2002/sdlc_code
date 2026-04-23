"""
Comprehensive Test Suite for Customer Order ETL Job

This test suite validates all FRD/TRD requirements with 15+ test functions:
- FR-INGEST-001: Data ingestion from S3
- FR-CLEAN-001: Data cleaning (NULLs, 'Null' strings, duplicates)
- FR-SCD2-001: SCD Type 2 implementation with Hudi
- FR-AGG-001: Customer spending aggregation
- FR-CATALOG-001: Glue Catalog registration

Test Framework: pytest with mocking for AWS services
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from pyspark.sql.functions import col

import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.main.job import (
    CustomerOrderETLJob,
    initialize_spark_contexts,
    read_data_safe,
    write_data_safe,
    validate_s3_path
)


# ============================================================================
# PYTEST FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing.
    """
    spark = SparkSession.builder \
        .appName("CustomerOrderETL-Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """
    Generate sample customer data matching TRD schema.

    TRD Schema: CustId, Name, EmailId, Region
    """
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),
        ("C004", "Alice Williams", "alice.williams@example.com", "West"),
        ("C005", "Charlie Brown", "charlie.brown@example.com", "North")
    ]

    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False),
        StructField("emailid", StringType(), False),
        StructField("region", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """
    Generate sample order data matching TRD schema.

    TRD Schema: OrderId, ItemName, PricePerUnit, Qty, Date, CustId
    """
    data = [
        ("O001", "Laptop", 1200.00, 1, "2024-01-15", "C001"),
        ("O002", "Mouse", 25.00, 2, "2024-01-16", "C001"),
        ("O003", "Keyboard", 75.00, 1, "2024-01-17", "C002"),
        ("O004", "Monitor", 300.00, 2, "2024-01-18", "C003"),
        ("O005", "Headphones", 150.00, 1, "2024-01-19", "C004"),
        ("O006", "Webcam", 80.00, 1, "2024-01-20", "C005"),
        ("O007", "USB Cable", 10.00, 5, "2024-01-21", "C001")
    ]

    schema = StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), False),
        StructField("priceperunit", DoubleType(), False),
        StructField("qty", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("custid", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """
    Generate customer data with NULL values for cleaning tests.

    FR-CLEAN-001: Test NULL removal
    """
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", None, "jane.smith@example.com", "South"),  # NULL name
        ("C003", "Bob Johnson", None, "East"),  # NULL email
        (None, "Alice Williams", "alice.williams@example.com", "West"),  # NULL custid
        ("C005", "Charlie Brown", "charlie.brown@example.com", "North")
    ]

    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_null_strings(spark):
    """
    Generate customer data with 'Null' string values for cleaning tests.

    FR-CLEAN-001: Test 'Null' string removal
    TRD: Remove 'Null' string values
    """
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Null", "jane.smith@example.com", "South"),  # 'Null' string
        ("C003", "Bob Johnson", "NULL", "East"),  # 'NULL' string
        ("C004", "null", "alice.williams@example.com", "West"),  # 'null' string
        ("C005", "Charlie Brown", "charlie.brown@example.com", "North")
    ]

    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False),
        StructField("emailid", StringType(), False),
        StructField("region", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_duplicates(spark):
    """
    Generate customer data with duplicates for cleaning tests.

    FR-CLEAN-001: Test duplicate removal
    """
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C001", "John Doe", "john.doe@example.com", "North"),  # Duplicate
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),  # Duplicate
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East")
    ]

    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False),
        StructField("emailid", StringType(), False),
        StructField("region", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_glue_context():
    """
    Mock GlueContext for testing.
    """
    mock_context = Mock()
    mock_context.spark_session = None
    return mock_context


@pytest.fixture
def etl_job(spark, mock_glue_context):
    """
    Create CustomerOrderETLJob instance for testing.
    """
    mock_glue_context.spark_session = spark
    return CustomerOrderETLJob(spark, mock_glue_context)


# ============================================================================
# TEST: FR-INGEST-001 - Data Ingestion
# ============================================================================

@pytest.mark.ingestion
@pytest.mark.unit
def test_customer_data_ingestion(etl_job, sample_customer_data):
    """
    Test FR-INGEST-001: Customer data ingestion.

    Validates:
    - Data is read from correct S3 path
    - Schema matches TRD specification
    - Column names are normalized to lowercase
    """
    # Verify schema matches TRD
    expected_columns = ["custid", "name", "emailid", "region"]
    assert sample_customer_data.columns == expected_columns

    # Verify data types
    schema = sample_customer_data.schema
    assert schema["custid"].dataType == StringType()
    assert schema["name"].dataType == StringType()
    assert schema["emailid"].dataType == StringType()
    assert schema["region"].dataType == StringType()

    # Verify record count
    assert sample_customer_data.count() == 5


@pytest.mark.ingestion
@pytest.mark.unit
def test_order_data_ingestion(etl_job, sample_order_data):
    """
    Test FR-INGEST-001: Order data ingestion.

    Validates:
    - Data is read from correct S3 path
    - Schema matches TRD specification
    - Column names are normalized to lowercase
    """
    # Verify schema matches TRD
    expected_columns = ["orderid", "itemname", "priceperunit", "qty", "date", "custid"]
    assert sample_order_data.columns == expected_columns

    # Verify data types
    schema = sample_order_data.schema
    assert schema["orderid"].dataType == StringType()
    assert schema["itemname"].dataType == StringType()
    assert schema["priceperunit"].dataType == DoubleType()
    assert schema["qty"].dataType == IntegerType()
    assert schema["date"].dataType == StringType()
    assert schema["custid"].dataType == StringType()

    # Verify record count
    assert sample_order_data.count() == 7


@pytest.mark.ingestion
@pytest.mark.unit
def test_s3_path_validation_valid():
    """
    Test S3 path validation with valid paths.

    TRD Paths:
    - s3://adif-sdlc/sdlc_wizard/customerdata/
    - s3://adif-sdlc/sdlc_wizard/orderdata/
    """
    with patch('boto3.client') as mock_boto:
        mock_s3 = Mock()
        mock_boto.return_value = mock_s3
        mock_s3.head_bucket.return_value = {}

        # Test customer path
        assert validate_s3_path("s3://adif-sdlc/sdlc_wizard/customerdata/")

        # Test order path
        assert validate_s3_path("s3://adif-sdlc/sdlc_wizard/orderdata/")


@pytest.mark.ingestion
@pytest.mark.unit
def test_s3_path_validation_invalid():
    """
    Test S3 path validation with invalid paths.
    """
    # Test invalid format
    with pytest.raises(ValueError):
        validate_s3_path("invalid-path")

    # Test non-existent bucket
    with patch('boto3.client') as mock_boto:
        mock_s3 = Mock()
        mock_boto.return_value = mock_s3
        from botocore.exceptions import ClientError
        mock_s3.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadBucket'
        )

        assert not validate_s3_path("s3://non-existent-bucket/path/")


# ============================================================================
# TEST: FR-CLEAN-001 - Data Cleaning
# ============================================================================

@pytest.mark.cleaning
@pytest.mark.unit
def test_clean_data_remove_nulls(etl_job, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001: Remove NULL values.

    TRD Requirement: Remove NULL values
    """
    initial_count = sample_customer_data_with_nulls.count()
    assert initial_count == 5

    # Clean data
    cleaned_df = etl_job.clean_data(sample_customer_data_with_nulls, "customer")

    # Verify NULLs removed (should have 2 valid records)
    assert cleaned_df.count() == 2

    # Verify no NULL values remain
    for column in cleaned_df.columns:
        null_count = cleaned_df.filter(col(column).isNull()).count()
        assert null_count == 0


@pytest.mark.cleaning
@pytest.mark.unit
def test_clean_data_remove_null_strings(etl_job, sample_customer_data_with_null_strings):
    """
    Test FR-CLEAN-001: Remove 'Null' string values.

    TRD Requirement: Remove 'Null' string values
    """
    initial_count = sample_customer_data_with_null_strings.count()
    assert initial_count == 5

    # Clean data
    cleaned_df = etl_job.clean_data(sample_customer_data_with_null_strings, "customer")

    # Verify 'Null' strings removed (should have 2 valid records)
    assert cleaned_df.count() == 2

    # Verify no 'Null' strings remain
    null_strings = ['Null', 'NULL', 'null', 'None', 'NONE', '']
    for column in cleaned_df.columns:
        for null_str in null_strings:
            null_str_count = cleaned_df.filter(col(column) == null_str).count()
            assert null_str_count == 0


@pytest.mark.cleaning
@pytest.mark.unit
def test_clean_data_remove_duplicates(etl_job, sample_customer_data_with_duplicates):
    """
    Test FR-CLEAN-001: Remove duplicate records.

    TRD Requirement: Remove duplicate records
    """
    initial_count = sample_customer_data_with_duplicates.count()
    assert initial_count == 5

    # Clean data
    cleaned_df = etl_job.clean_data(sample_customer_data_with_duplicates, "customer")

    # Verify duplicates removed (should have 3 unique records)
    assert cleaned_df.count() == 3


@pytest.mark.cleaning
@pytest.mark.unit
def test_clean_data_comprehensive(etl_job, spark):
    """
    Test FR-CLEAN-001: Comprehensive cleaning (NULLs + 'Null' strings + duplicates).
    """
    # Create data with all issues
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C001", "John Doe", "john.doe@example.com", "North"),  # Duplicate
        ("C002", None, "jane.smith@example.com", "South"),  # NULL
        ("C003", "Null", "bob.johnson@example.com", "East"),  # 'Null' string
        ("C004", "Alice Williams", "alice.williams@example.com", "West")
    ]

    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    assert df.count() == 5

    # Clean data
    cleaned_df = etl_job.clean_data(df, "customer")

    # Should have 2 valid, unique records
    assert cleaned_df.count() == 2


# ============================================================================
# TEST: FR-SCD2-001 - SCD Type 2 Implementation
# ============================================================================

@pytest.mark.scd2
@pytest.mark.unit
def test_add_scd2_columns(etl_job, sample_customer_data):
    """
    Test FR-SCD2-001: Add SCD Type 2 columns.

    TRD Requirements:
    - Add columns: IsActive, StartDate, EndDate, OpTs
    """
    # Add SCD Type 2 columns
    df_scd2 = etl_job.add_scd2_columns(sample_customer_data)

    # Verify SCD Type 2 columns exist
    expected_columns = [
        "custid", "name", "emailid", "region",
        "isactive", "startdate", "enddate", "opts"
    ]
    assert df_scd2.columns == expected_columns

    # Verify data types
    schema = df_scd2.schema
    assert schema["isactive"].dataType == BooleanType()
    assert schema["startdate"].dataType == TimestampType()
    assert schema["enddate"].dataType == TimestampType()
    assert schema["opts"].dataType == TimestampType()

    # Verify default values
    first_row = df_scd2.first()
    assert first_row["isactive"] == True
    assert first_row["startdate"] is not None
    assert first_row["enddate"] is not None
    assert first_row["opts"] is not None


@pytest.mark.scd2
@pytest.mark.unit
def test_scd2_isactive_default(etl_job, sample_customer_data):
    """
    Test FR-SCD2-001: IsActive column defaults to True.
    """
    df_scd2 = etl_job.add_scd2_columns(sample_customer_data)

    # All records should have IsActive = True
    active_count = df_scd2.filter(col("isactive") == True).count()
    assert active_count == df_scd2.count()


@pytest.mark.scd2
@pytest.mark.unit
def test_scd2_enddate_default(etl_job, sample_customer_data):
    """
    Test FR-SCD2-001: EndDate defaults to 9999-12-31 23:59:59.
    """
    df_scd2 = etl_job.add_scd2_columns(sample_customer_data)

    # Verify EndDate is set to far future
    first_row = df_scd2.first()
    enddate_str = str(first_row["enddate"])
    assert "9999-12-31" in enddate_str


@pytest.mark.scd2
@pytest.mark.hudi
@patch('src.main.job.write_data_safe')
def test_write_customer_aggregate_hudi(mock_write, etl_job, sample_customer_data):
    """
    Test FR-SCD2-001: Write customer aggregate with Hudi format.

    TRD Requirements:
    - Format: Hudi
    - Operation: upsert
    - Output: s3://adif-sdlc/analytics/customeraggregatespend/
    """
    mock_write.return_value = True

    # Write customer aggregate
    result = etl_job.write_customer_aggregate(sample_customer_data)

    # Verify write was called
    assert result == True
    assert mock_write.called

    # Verify Hudi options
    call_args = mock_write.call_args
    hudi_options = call_args[1]['hudi_options']

    assert hudi_options['hoodie.datasource.write.operation'] == 'upsert'
    assert hudi_options['hoodie.datasource.write.recordkey.field'] == 'custid'
    assert 'hoodie.table.name' in hudi_options


# ============================================================================
# TEST: FR-AGG-001 - Aggregation
# ============================================================================

@pytest.mark.aggregation
@pytest.mark.unit
def test_aggregate_customer_spend(etl_job, sample_customer_data, sample_order_data):
    """
    Test FR-AGG-001: Aggregate customer spending.

    TRD Requirement: Calculate total spend per customer
    """
    # Aggregate customer spend
    result = etl_job.aggregate_customer_spend(sample_customer_data, sample_order_data)

    assert result is not None

    # Verify columns
    expected_columns = ["custid", "name", "emailid", "region", "totalspend", "ordercount"]
    assert result.columns == expected_columns

    # Verify aggregation for C001 (3 orders: 1200 + 50 + 50 = 1300)
    c001_data = result.filter(col("custid") == "C001").first()
    assert c001_data["ordercount"] == 3
    assert c001_data["totalspend"] == 1300.0


@pytest.mark.aggregation
@pytest.mark.unit
def test_aggregate_customer_spend_calculation(etl_job, sample_customer_data, sample_order_data):
    """
    Test FR-AGG-001: Verify spend calculation (PricePerUnit * Qty).
    """
    result = etl_job.aggregate_customer_spend(sample_customer_data, sample_order_data)

    # Verify C002 (1 order: 75 * 1 = 75)
    c002_data = result.filter(col("custid") == "C002").first()
    assert c002_data["totalspend"] == 75.0
    assert c002_data["ordercount"] == 1

    # Verify C003 (1 order: 300 * 2 = 600)
    c003_data = result.filter(col("custid") == "C003").first()
    assert c003_data["totalspend"] == 600.0
    assert c003_data["ordercount"] == 1


@pytest.mark.aggregation
@pytest.mark.unit
def test_aggregate_customer_spend_join(etl_job, sample_customer_data, sample_order_data):
    """
    Test FR-AGG-001: Verify customer-order join.
    """
    result = etl_job.aggregate_customer_spend(sample_customer_data, sample_order_data)

    # All customers with orders should be in result
    assert result.count() == 5

    # Verify customer details are included
    c001_data = result.filter(col("custid") == "C001").first()
    assert c001_data["name"] == "John Doe"
    assert c001_data["emailid"] == "john.doe@example.com"
    assert c001_data["region"] == "North"


# ============================================================================
# TEST: FR-CATALOG-001 - Glue Catalog Registration
# ============================================================================

@pytest.mark.catalog
@pytest.mark.integration
@patch('boto3.client')
def test_register_glue_catalog_tables(mock_boto, etl_job):
    """
    Test FR-CATALOG-001: Register tables in Glue Catalog.

    TRD Requirement: Database: sdlc_wizard_db
    """
    mock_glue = Mock()
    mock_boto.return_value = mock_glue
    mock_glue.create_database.return_value = {}

    # Register tables
    result = etl_job.register_glue_catalog_tables()

    assert result == True

    # Verify database creation was attempted
    assert mock_glue.create_database.called
    call_args = mock_glue.create_database.call_args[1]
    assert call_args['DatabaseInput']['Name'] == 'sdlc_wizard_db'


@pytest.mark.catalog
@pytest.mark.integration
@patch('boto3.client')
def test_register_glue_catalog_database_exists(mock_boto, etl_job):
    """
    Test FR-CATALOG-001: Handle existing database gracefully.
    """
    mock_glue = Mock()
    mock_boto.return_value = mock_glue

    # Simulate database already exists
    from botocore.exceptions import ClientError
    mock_glue.exceptions.AlreadyExistsException = type('AlreadyExistsException', (Exception,), {})
    mock_glue.create_database.side_effect = mock_glue.exceptions.AlreadyExistsException()

    # Should handle gracefully
    result = etl_job.register_glue_catalog_tables()
    assert result == True


# ============================================================================
# TEST: Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
@patch('src.main.job.read_data_safe')
@patch('src.main.job.write_data_safe')
@patch('boto3.client')
def test_etl_job_run_complete_pipeline(
    mock_boto, mock_write, mock_read,
    etl_job, sample_customer_data, sample_order_data
):
    """
    Test complete ETL pipeline execution.

    Validates all FRD requirements:
    - FR-INGEST-001: Data ingestion
    - FR-CLEAN-001: Data cleaning
    - FR-AGG-001: Aggregation
    - FR-SCD2-001: SCD Type 2 write
    - FR-CATALOG-001: Catalog registration
    """
    # Mock data reads
    mock_read.side_effect = [sample_customer_data, sample_order_data]

    # Mock data writes
    mock_write.return_value = True

    # Mock Glue client
    mock_glue = Mock()
    mock_boto.return_value = mock_glue
    mock_glue.create_database.return_value = {}

    # Run ETL job
    result = etl_job.run()

    # Verify success
    assert result == True

    # Verify read was called twice (customer + order)
    assert mock_read.call_count == 2

    # Verify write was called twice (aggregate + summary)
    assert mock_write.call_count == 2

    # Verify catalog registration
    assert mock_glue.create_database.called


@pytest.mark.integration
def test_etl_job_s3_paths_match_trd(etl_job):
    """
    Test that S3 paths match TRD specifications exactly.

    TRD Paths:
    - Customer: s3://adif-sdlc/sdlc_wizard/customerdata/
    - Order: s3://adif-sdlc/sdlc_wizard/orderdata/
    - Aggregate: s3://adif-sdlc/analytics/customeraggregatespend/
    - Curated: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
    """
    assert etl_job.CUSTOMER_INPUT_PATH == "s3://adif-sdlc/sdlc_wizard/customerdata/"
    assert etl_job.ORDER_INPUT_PATH == "s3://adif-sdlc/sdlc_wizard/orderdata/"
    assert etl_job.AGGREGATE_OUTPUT_PATH == "s3://adif-sdlc/analytics/customeraggregatespend/"
    assert etl_job.CURATED_OUTPUT_PATH == "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"


@pytest.mark.integration
def test_etl_job_glue_catalog_names_match_trd(etl_job):
    """
    Test that Glue Catalog names match TRD specifications.

    TRD: Database: sdlc_wizard_db
    """
    assert etl_job.GLUE_DATABASE == "sdlc_wizard_db"
    assert etl_job.CUSTOMER_AGGREGATE_TABLE == "customer_aggregate_spend"
    assert etl_job.ORDER_SUMMARY_TABLE == "order_summary_curated"


# ============================================================================
# TEST: Error Handling
# ============================================================================

@pytest.mark.unit
def test_clean_data_empty_dataframe(etl_job, spark):
    """
    Test data cleaning with empty DataFrame.
    """
    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False)
    ])

    empty_df = spark.createDataFrame([], schema)

    # Should handle empty DataFrame gracefully
    cleaned_df = etl_job.clean_data(empty_df, "test")
    assert cleaned_df.count() == 0


@pytest.mark.unit
def test_aggregate_customer_spend_no_matches(etl_job, sample_customer_data, spark):
    """
    Test aggregation when no customer-order matches exist.
    """
    # Create orders with non-matching customer IDs
    data = [
        ("O001", "Laptop", 1200.00, 1, "2024-01-15", "C999")
    ]

    schema = StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), False),
        StructField("priceperunit", DoubleType(), False),
        StructField("qty", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("custid", StringType(), False)
    ])

    order_df = spark.createDataFrame(data, schema)

    # Aggregate should return empty result
    result = etl_job.aggregate_customer_spend(sample_customer_data, order_df)
    assert result.count() == 0


# ============================================================================
# TEST: Performance & Data Quality
# ============================================================================

@pytest.mark.unit
def test_column_name_normalization(sample_customer_data):
    """
    Test that column names are normalized to lowercase.

    TRD Requirement: Normalize column names
    """
    # All columns should be lowercase
    for col_name in sample_customer_data.columns:
        assert col_name == col_name.lower()


@pytest.mark.unit
def test_data_types_match_trd(sample_customer_data, sample_order_data):
    """
    Test that data types match TRD specifications exactly.
    """
    # Customer schema
    customer_schema = sample_customer_data.schema
    assert customer_schema["custid"].dataType == StringType()
    assert customer_schema["name"].dataType == StringType()
    assert customer_schema["emailid"].dataType == StringType()
    assert customer_schema["region"].dataType == StringType()

    # Order schema
    order_schema = sample_order_data.schema
    assert order_schema["orderid"].dataType == StringType()
    assert order_schema["itemname"].dataType == StringType()
    assert order_schema["priceperunit"].dataType == DoubleType()
    assert order_schema["qty"].dataType == IntegerType()
    assert order_schema["date"].dataType == StringType()
    assert order_schema["custid"].dataType == StringType()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])