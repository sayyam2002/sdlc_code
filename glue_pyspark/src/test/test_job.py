"""
Comprehensive test suite for Customer Order Pipeline
Tests all transformations, data quality rules, and SCD logic
"""

import sys
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)

# Import from main package
from main.job import (
    get_job_parameters,
    initialize_spark_contexts,
    validate_s3_access,
    DataReader,
    DataCleaner,
    S3Writer,
    CatalogManager,
    SCDProcessor,
    AggregationEngine
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test-customer-order-pipeline") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    # Don't stop if builtins.spark exists
    if 'builtins' not in dir() or not hasattr(__builtins__, 'spark'):
        spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North")
    ]

    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False),
        StructField("emailid", StringType(), False),
        StructField("region", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with null values for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        ("C004", "Alice Brown", "alice@example.com", None),
        ("C005", "Charlie Wilson", "charlie@example.com", "North")
    ]

    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_duplicates(spark):
    """Create sample customer data with duplicates for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C002", "Jane Smith", "jane@example.com", "South")  # Duplicate
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
    """Create sample order data for testing"""
    data = [
        ("O001", "Laptop", Decimal("999.99"), 2, date(2024, 1, 15), "C001"),
        ("O002", "Mouse", Decimal("25.50"), 5, date(2024, 1, 15), "C001"),
        ("O003", "Keyboard", Decimal("75.00"), 3, date(2024, 1, 16), "C002"),
        ("O004", "Monitor", Decimal("299.99"), 1, date(2024, 1, 16), "C002"),
        ("O005", "Headset", Decimal("89.99"), 2, date(2024, 1, 17), "C003")
    ]

    schema = StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), False),
        StructField("priceperunit", DecimalType(10, 2), False),
        StructField("qty", IntegerType(), False),
        StructField("date", DateType(), False),
        StructField("custid", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


def test_get_job_parameters():
    """Test job parameter loading"""
    params = get_job_parameters()

    assert params is not None
    assert "customer_source_path" in params
    assert "order_source_path" in params
    assert "curated_target_path" in params
    assert "analytics_target_path" in params
    assert "catalog_database" in params

    # Verify S3 paths
    assert params["customer_source_path"].startswith("s3://")
    assert params["order_source_path"].startswith("s3://")
    assert params["curated_target_path"].startswith("s3://")
    assert params["analytics_target_path"].startswith("s3://")


@patch('main.job.Job')
@patch('main.job.GlueContext')
@patch('main.job.SparkContext')
@patch('main.job.getResolvedOptions')
def test_initialize_spark_contexts(mock_resolved_options, mock_spark_context, mock_glue_context, mock_job):
    """Test Spark context initialization"""
    mock_resolved_options.return_value = {'JOB_NAME': 'test-job'}

    mock_sc = MagicMock()
    mock_spark_context.getOrCreate.return_value = mock_sc

    mock_glue_ctx = MagicMock()
    mock_glue_context.return_value = mock_glue_ctx

    mock_spark_session = MagicMock()
    mock_glue_ctx.spark_session = mock_spark_session

    mock_job_instance = MagicMock()
    mock_job.return_value = mock_job_instance

    spark, glueContext, job, args = initialize_spark_contexts()

    assert spark is not None
    assert glueContext is not None
    assert job is not None
    assert args is not None
    assert args['JOB_NAME'] == 'test-job'


def test_validate_s3_access(spark):
    """Test S3 path validation"""
    # Valid S3 paths
    assert validate_s3_access(spark, "s3://bucket/path/") == True
    assert validate_s3_access(spark, "s3://adif-sdlc/data/") == True

    # Invalid paths
    assert validate_s3_access(spark, "file:///local/path") == False
    assert validate_s3_access(spark, "/local/path") == False
    assert validate_s3_access(spark, "") == False
    assert validate_s3_access(spark, None) == False


def test_data_reader_customer(spark, sample_customer_data):
    """Test customer data reading with mocked S3"""
    reader = DataReader(spark)

    # Adjust sample data to match actual schema with CamelCase
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North")
    ]

    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("EmailId", StringType(), False),
        StructField("Region", StringType(), False)
    ])

    sample_data_camelcase = spark.createDataFrame(data, schema)

    with patch.object(DataReader, '_read_csv', return_value=sample_data_camelcase):
        df = reader.read_customer_data("s3://test-bucket/customer/")

        assert df is not None
        assert df.count() == 5
        assert set(df.columns) == {"CustId", "Name", "EmailId", "Region"}

        # Verify schema
        assert df.schema["CustId"].dataType == StringType()
        assert df.schema["Name"].dataType == StringType()


def test_data_reader_order(spark, sample_order_data):
    """Test order data reading with mocked S3"""
    reader = DataReader(spark)

    # Create raw order data (before type casting)
    raw_data = [
        ("O001", "Laptop", "999.99", "2", "2024-01-15", "C001"),
        ("O002", "Mouse", "25.50", "5", "2024-01-15", "C001"),
        ("O003", "Keyboard", "75.00", "3", "2024-01-16", "C002")
    ]

    raw_schema = StructType([
        StructField("OrderId", StringType(), False),
        StructField("ItemName", StringType(), False),
        StructField("PricePerUnit", StringType(), False),
        StructField("Qty", StringType(), False),
        StructField("Date", StringType(), False),
        StructField("CustId", StringType(), False)
    ])

    raw_df = spark.createDataFrame(raw_data, raw_schema)

    with patch.object(DataReader, '_read_csv', return_value=raw_df):
        df = reader.read_order_data("s3://test-bucket/order/")

        assert df is not None
        assert df.count() == 3

        # Verify type casting
        assert df.schema["PricePerUnit"].dataType == DecimalType(10, 2)
        assert df.schema["Qty"].dataType == IntegerType()
        assert df.schema["Date"].dataType == DateType()


def test_data_cleaner_remove_nulls(spark, sample_customer_data_with_nulls):
    """Test null value removal"""
    df_clean = DataCleaner.remove_nulls(sample_customer_data_with_nulls)

    # Should remove 3 records with nulls
    assert df_clean.count() == 2

    # Verify no nulls remain
    for row in df_clean.collect():
        assert row