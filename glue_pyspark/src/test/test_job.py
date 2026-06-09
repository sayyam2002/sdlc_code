"""
Unit tests for AWS Glue PySpark ETL Job - Customer Order Processing

Tests cover:
- Data ingestion from S3
- Data cleaning transformations
- Catalog registration
- SCD Type 2 implementation
- Customer aggregate spend generation
"""

import sys
import builtins
from unittest.mock import patch, MagicMock
from decimal import Decimal
from datetime import datetime, date

# Import test dependencies
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)

# Import job functions
from main.job import (
    get_job_parameters,
    initialize_spark_contexts,
    validate_s3_access,
    DataReader,
    DataWriter,
    read_data_safe,
    write_data_safe,
    get_customer_schema,
    get_order_schema,
    ingest_customer_data,
    ingest_order_data,
    clean_customer_data,
    clean_order_data,
    add_scd2_columns,
    register_catalog_table,
    generate_order_summary_scd2,
    generate_customer_aggregate_spend
)


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("test_customer_order_processing") \
        .getOrCreate()

    # Set as builtin for job initialization
    if isinstance(__builtins__, dict):
        __builtins__['spark'] = spark
    else:
        __builtins__.spark = spark

    yield spark

    # Cleanup
    if isinstance(__builtins__, dict):
        if 'spark' in __builtins__:
            del __builtins__['spark']
    else:
        if hasattr(__builtins__, 'spark'):
            delattr(__builtins__, 'spark')


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
    schema = get_customer_schema()
    df = spark.createDataFrame(data, schema)
    return df.toDF(*[c.lower() for c in df.columns])


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with null values for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "Null", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", None, "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North"),
        ("C002", "Jane Smith", "Null", "South")  # Duplicate
    ]
    schema = get_customer_schema()
    df = spark.createDataFrame(data, schema)
    return df.toDF(*[c.lower() for c in df.columns])


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing"""
    data = [
        ("O001", "Laptop", Decimal("999.99"), 1, "2024-01-15", "C001"),
        ("O002", "Mouse", Decimal("25.50"), 2, "2024-01-16", "C001"),
        ("O003", "Keyboard", Decimal("75.00"), 1, "2024-01-17", "C002"),
        ("O004", "Monitor", Decimal("299.99"), 2, "2024-01-18", "C003"),
        ("O005", "Headphones", Decimal("150.00"), 1, "2024-01-19", "C004")
    ]
    schema = StructType([
        StructField("OrderId", StringType(), False),
        StructField("ItemName", StringType(), False),
        StructField("PricePerUnit", DecimalType(10, 2), False),
        StructField("Qty", IntegerType(), False),
        StructField("Date", StringType(), False),
        StructField("CustId", StringType(), False)
    ])
    df = spark.createDataFrame(data, schema)
    df = df.toDF(*[c.lower() for c in df.columns])
    # Convert date string to date type
    from pyspark.sql.functions import to_date, col
    df = df.withColumn("date", to_date(col("date")))
    return df


@pytest.fixture
def sample_order_data_with_nulls(spark):
    """Create sample order data with null values for testing"""
    data = [
        ("O001", "Laptop", Decimal("999.99"), 1, "2024-01-15", "C001"),
        ("O002", "Mouse", Decimal("25.50"), 2, "2024-01-16", "C001"),
        ("O003", "Null", Decimal("75.00"), 1, "2024-01-17", "C002"),
        ("O004", "Monitor", Decimal("299.99"), 2, "2024-01-18", None),
        ("O005", "Headphones", Decimal("150.00"), 1, "2024-01-19", "C004"),
        ("O002", "Mouse", Decimal("25.50"), 2, "2024-01-16", "C001")  # Duplicate
    ]
    schema = StructType([
        StructField("OrderId", StringType(), False),
        StructField("ItemName", StringType(), False),
        StructField("PricePerUnit", DecimalType(10, 2), False),
        StructField("Qty", IntegerType(), False),
        StructField("Date", StringType(), False),
        StructField("CustId", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df = df.toDF(*[c.lower() for c in df.columns])
    # Convert date string to date type
    from pyspark.sql.functions import to_date, col
    df = df.withColumn("date", to_date(col("date")))
    return df


def test_get_job_parameters():
    """Test loading job parameters from config"""
    params = get_job_parameters()

    assert params is not None
    assert 'customer_source_path' in params
    assert 'order_source_path' in params
    assert params['customer_source_path'].startswith('s3://')
    assert params['order_source_path'].startswith('s3://')


def test_initialize_spark_contexts(spark):
    """Test Spark context initialization"""
    sc, glueContext, spark_session, job = initialize_spark_contexts()

    assert sc is not None
    assert glueContext is not None
    assert spark_session is not None
    assert job is not None


def test_validate_s3_access(spark):
    """Test S3 path validation"""
    valid_path = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    invalid_path = "/local/path"

    assert validate_s3_access(spark, valid_path) == True
    assert validate_s3_access(spark, invalid_path) == False


def test_get_customer_schema():
    """Test customer schema definition"""
    schema = get_customer_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[1].name == "Name"
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[3].name == "Region"


def test_get_order_schema():
    """Test order schema definition"""
    schema = get_order_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 6
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[2].name == "PricePerUnit"
    assert isinstance(schema.fields[2].dataType, DecimalType)


def test_ingest_customer_data(spark, sample_customer_data):
    """Test TR-INGEST-001: Customer data ingestion"""
    params = get_job_parameters()
    reader = DataReader(spark)

    # Mock the read_csv method
    with patch.object(DataReader, 'read_csv', return_value=sample_customer_data):
        result_df = ingest_customer_data(reader, params)

        assert result_df is not None
        assert result_df.count() == 5
        assert 'custid' in result_df.columns
        assert 'name' in result_df.columns


def test_ingest_order_data(spark, sample_order_data):
    """Test TR-INGEST-002: Order data ingestion"""
    params = get_job_parameters()
    reader = DataReader(spark)

    # Mock the read_csv method
    with patch.object(DataReader, 'read_csv', return_value=sample_order_data):
        result_df = ingest_order_data(reader, params)

        assert result_df is not None
        assert result_df.count() == 5
        assert 'orderid' in result_df.columns
        assert 'priceperunit' in result_df.columns


def test_clean_customer_data(spark, sample_customer_data_with_nulls):
    """Test TR-CLEAN-001: Customer data cleaning"""
    params = get_job_parameters()

    result_df = clean_customer_data(sample_customer_data_with_nulls, params)

    # Should remove 2 rows with nulls and 1 duplicate
    assert result_df.count() < sample_customer_data_with_nulls.count()

    # Verify no null values remain
    null_count = result_df.filter(result_df.name.isNull()).count()
    assert null_count == 0

    # Verify no "Null" string values remain
    null_string_count = result_df.filter(result_df.emailid == "Null").count()
    assert null_string_count == 0


def test_clean_order_data(spark, sample_order_data_with_nulls):
    """Test TR-CLEAN-002: Order data cleaning"""
    params = get_job_parameters()

    result_df = clean_order_data(sample_order_data_with_nulls, params)

    # Should remove rows with nulls and duplicates
    assert result_df.count() < sample_order_data_with_nulls.count()

    # Verify no null values remain
    null_count = result_df.filter(result_df.custid.isNull()).count()
    assert null_count == 0

    # Verify no "Null" string values remain
    null_string_count = result_df.filter(result_df.itemname ==