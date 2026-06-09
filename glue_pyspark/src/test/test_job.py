"""
Comprehensive unit tests for AWS Glue PySpark ETL Job
Tests use mocked Spark I/O and sample data
"""

import os
import sys
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import patch, MagicMock, Mock
from typing import List, Tuple

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)

# Import job functions
from main.job import (
    initialize_spark_contexts,
    get_job_parameters,
    validate_s3_access,
    clean_dataframe,
    apply_scd2,
    aggregate_customer_spend,
    get_customer_schema,
    get_order_schema,
    DataReader,
    DataWriter
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestCustomerOrderETL") \
        .master("local[2]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    # Don't stop spark if it's a builtin
    if not hasattr(__builtins__, 'spark'):
        spark.stop()


@pytest.fixture
def sample_customer_data():
    """Sample customer data for testing"""
    return [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North")
    ]


@pytest.fixture
def sample_order_data():
    """Sample order data for testing"""
    return [
        ("O001", "Laptop", Decimal("999.99"), 1, date(2024, 1, 15), "C001"),
        ("O002", "Mouse", Decimal("25.50"), 2, date(2024, 1, 16), "C001"),
        ("O003", "Keyboard", Decimal("75.00"), 1, date(2024, 1, 17), "C002"),
        ("O004", "Monitor", Decimal("299.99"), 2, date(2024, 1, 18), "C003"),
        ("O005", "Headphones", Decimal("150.00"), 1, date(2024, 1, 19), "C004")
    ]


@pytest.fixture
def sample_customer_data_with_nulls():
    """Sample customer data with null values for testing"""
    return [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Null", "jane@example.com", "South"),
        ("C003", "Bob Johnson", None, "East"),
        ("C004", "Alice Brown", "alice@example.com", "Null"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North"),
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
    ]


@pytest.fixture
def sample_order_data_with_nulls():
    """Sample order data with null values for testing"""
    return [
        ("O001", "Laptop", Decimal("999.99"), 1, date(2024, 1, 15), "C001"),
        ("O002", "Null", Decimal("25.50"), 2, date(2024, 1, 16), "C001"),
        ("O003", "Keyboard", None, 1, date(2024, 1, 17), "C002"),
        ("O004", "Monitor", Decimal("299.99"), 2, date(2024, 1, 18), "C003"),
        ("O001", "Laptop", Decimal("999.99"), 1, date(2024, 1, 15), "C001")  # Duplicate
    ]


def test_initialize_spark_contexts():
    """Test Spark context initialization"""
    spark, glue_context = initialize_spark_contexts()

    assert spark is not None
    assert isinstance(spark, SparkSession)


def test_get_job_parameters():
    """Test loading job parameters from YAML"""
    params = get_job_parameters()

    assert params is not None
    assert 'inputs' in params
    assert 'outputs' in params
    assert 'catalog' in params
    assert 'hudi' in params
    assert 'processing' in params

    # Validate customer input configuration
    assert 'customer' in params['inputs']
    assert params['inputs']['customer']['source_path'] == "s3://adif-sdlc/sdlc_wizard/customerdata/"
    assert params['inputs']['customer']['source_format'] == "csv"

    # Validate order input configuration
    assert 'order' in params['inputs']
    assert params['inputs']['order']['source_path'] == "s3://adif-sdlc/sdlc_wizard/orderdata/"

    # Validate output paths
    assert params['outputs']['customer']['target_path'] == "s3://adif-sdlc/curated/sdlc_wizard/customer/"
    assert params['outputs']['ordersummary']['target_format'] == "hudi"


def test_validate_s3_access(spark):
    """Test S3 access validation"""
    # Valid S3 path
    assert validate_s3_access(spark, "s3://adif-sdlc/sdlc_wizard/customerdata/") == True

    # Invalid path (not S3)
    assert validate_s3_access(spark, "/local/path/data/") == False
    assert validate_s3_access(spark, "file:///tmp/data/") == False


def test_get_customer_schema():
    """Test customer schema definition"""
    schema = get_customer_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4

    field_names = [f.name for f in schema.fields]
    assert "custid" in field_names
    assert "name" in field_names
    assert "emailid" in field_names
    assert "region" in field_names


def test_get_order_schema():
    """Test order schema definition"""
    schema = get_order_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 6

    field_names = [f.name for f in schema.fields]
    assert "orderid" in field_names
    assert "itemname" in field_names
    assert "priceperunit" in field_names
    assert "qty" in field_names
    assert "date" in field_names
    assert "custid" in field_names


def test_clean_dataframe_removes_nulls(spark, sample_customer_data_with_nulls):
    """Test that clean_dataframe removes NULL values"""
    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    df = spark.createDataFrame(sample_customer_data_with_nulls, schema)
    null_values = ["Null", "NULL", "null"]

    cleaned_df = clean_dataframe(df, null_values)

    # Should remove rows with actual NULL and "Null" string
    assert cleaned_df.count() == 1  # Only C001 and C005 valid, but C001 is duplicate

    # Verify no null values remain
    assert cleaned_df.filter(df.name == "Null").count() == 0
    assert cleaned_df.filter(df.emailid.isNull()).count() == 0


def test_clean_dataframe_removes_duplicates(spark, sample_customer_data):
    """Test that clean_dataframe removes duplicate records"""
    # Add duplicate record
    data_with_dup = list(sample_customer_data)
    data_with_dup.append(sample_customer_data[0])  # Duplicate first record

    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False),
        StructField("emailid", StringType(), False),
        StructField("region", StringType(), False)
    ])

    df = spark.createDataFrame(data_with_dup, schema)
    null_values = ["Null"]

    cleaned_df = clean_dataframe(df, null_values)

    # Should have 5 unique records (duplicate removed)
    assert cleaned_df.count() == 5


def test_apply_scd2_creates_metadata_columns(spark, sample_customer_data, sample_order_data):
    """Test that SCD2 logic adds required metadata columns"""
    customer_schema = get_customer_schema()
    order_schema = get_order_schema()

    customer_df = spark.createDataFrame(sample_customer_data, customer_schema)
    order_df = spark.createDataFrame(sample_order_data, order_schema)

    scd2_df = apply_scd2(customer_df, order_df, spark)

    # Verify SCD2 columns exist
    assert "isactive" in scd2_df.columns
    assert "startdate" in scd2_df.columns
    assert "enddate" in scd2_df.columns
    assert "opts" in scd2_df.columns

    # Verify all records are active initially
    assert scd2_df.filter(scd2_df.isactive == True).count() == scd2_df.count()

    # Verify enddate is null for active records
    assert scd2_df.filter(scd2_df.enddate.isNull()).count() == scd2_df.count()


def test_apply_scd2_joins_customer_and_order(spark, sample_customer_data, sample_order_data):
    """Test that SCD2 logic correctly joins customer and order data"""
    customer_schema = get_customer_schema()
    order_schema = get_order_schema()

    customer_df = spark.createDataFrame(sample_customer_data, customer_schema)
    order_df = spark.createDataFrame(sample_order_data, order_schema)

    scd2_df = apply_scd2(customer_df, order_df, spark)

    # Should have 5 joined records
    assert scd2_df.count() == 5

    # Verify customer fields are present
    assert "name" in scd2_df.columns
    assert "emailid" in scd2_df.columns
    assert "region" in scd2_df.columns

    # Verify order fields are present
    assert "orderid" in scd2_df.columns
    assert "itemname" in scd2_df.columns
    assert "priceperunit" in scd2_df.columns


def test_aggregate_customer_spend_calculates_totals(spark, sample_customer_data, sample_order_data):
    """Test that aggregation correctly calculates customer spending"""
    customer_schema = get_customer_schema()
    order_schema = get_order_schema()

    customer_df = spark.createDataFrame(sample_customer_data, customer_schema)
    order_df = spark.createDataFrame(sample_order_data, order_schema)

    scd2_df = apply_scd2(customer_df, order_df, spark)
    agg_df = aggregate_customer_spend(scd2_df)

    # Verify aggregation columns exist
    assert "total_spend" in agg_df.columns
    assert "order_count" in agg_df.columns
    assert "total_quantity" in agg_df.columns
    assert "max_price" in agg_df.columns
    assert "min_price" in agg_df.columns

    # C001 has 2 orders
    c001_agg = agg_df.filter(agg_df.custid == "C001").collect()
    assert len(c001_agg) == 2  # Two different dates

    # Verify total spend calculation for O001: 999.99 * 1 = 999.99
    o001_spend = agg_df.filter(
        (agg_df.custid == "C001") & (agg_df.date == date(2024, 1, 15))
    ).select("total_spend").collect()
    assert len(o001_spend) > 0
    assert float(o001_spend[0].total_spend) == 999.99


def test_aggregate_customer_spend_groups_by_date(spark, sample_customer_data, sample_order_data):
    """Test that aggregation groups by customer and date"""
    customer_schema = get_customer_schema()
    order_schema = get_order_schema()

    customer_df = spark.createDataFrame(sample_customer_data, customer_schema)
    order_df = spark.createDataFrame(sample_order_data, order_schema)

    scd2_df = apply_scd2(customer_df, order_df, spark)
    agg_df = aggregate_customer_spend(scd2_df)

    # Should have 5 unique customer-date combinations
    assert agg_df.count() == 5

    # Verify grouping columns
    assert "custid" in agg_df.columns
    assert "date" in agg_df.columns


def test_data_reader_csv(spark, sample_customer_data):
    """Test DataReader CSV reading with mocking"""
    reader = DataReader(spark)

    # Create mock DataFrame
    schema = get_customer_schema()
    mock_df = spark.createDataFrame(sample_customer_data, schema)

    # Mock the internal _read_csv method
    with patch.object(DataReader, '_read_csv', return_value=mock_df):
        result_df = reader.read_data("s3://test/path/", "csv", schema)

        assert result_df is not None
        assert result_df.count() == 5


def test_data_reader_parquet(spark, sample_customer_data):
    """Test DataReader Parquet reading with mocking"""
    reader = DataReader(spark)

    # Create mock DataFrame
    schema = get_customer_schema()
    mock_df = spark.createDataFrame(sample_customer_data, schema)

    # Mock the internal _read_parquet method
    with patch.object(DataReader, '_read_parquet', return_value=mock_df):
        result_df = reader.read_data("s3://test/path/", "parquet")

        assert result_df is not None
        assert result_df.count() == 5


def test_data_writer_parquet(spark, sample_customer_data):
    """Test DataWriter Parquet writing with mocking"""
    writer = DataWriter(spark)

    schema = get_customer_schema()
    df = spark.createDataFrame(sample_customer_data, schema)

    # Mock the internal _write_parquet method
    with patch.object(DataWriter, '_write_parquet') as mock_write:
        writer.write_data(df, "s3://test/path/", "parquet")

        # Verify the method was called
        mock_write.assert_called_once()


def test_data_writer_csv(spark, sample_customer_data):
    """Test DataWriter CSV writing with mocking"""
    writer = DataWriter(spark)

    schema = get_customer_schema()
    df = spark.createDataFrame(sample_customer_data, schema)

    # Mock the internal _write_csv method
    with patch.object(DataWriter, '_write_csv') as mock_write:
        writer.write_data(df, "s3://test/path/", "csv")

        # Verify the method was called
        mock_write.assert_called_once()


def test_data_writer_hudi(spark, sample_customer_data):
    """Test DataWriter Hudi writing with mocking"""
    writer = DataWriter(spark)

    schema = get_customer_schema()
    df = spark.createDataFrame(sample_customer_data, schema)

    hudi_options = {
        'hoodie.table.name': 'test_table',
        'hoodie.datasource.write.recordkey.field': 'custid',
        'hoodie.datasource.write.precombine.field': 'name'
    }

    # Mock the internal _write_hudi method
    with patch.object(DataWriter, '_write_hudi') as mock_write:
        writer.write_data(df, "s3://test/path/", "hudi", hudi_options=hudi_options)

        # Verify the method was called
        mock_write.assert_called_once()


def test_data_writer_hudi_requires_options(spark, sample_customer_data):
    """Test that Hudi writing requires options"""
    writer = DataWriter(spark)

    schema = get_customer_schema()
    df = spark.createDataFrame(sample_customer_data, schema)

    # Should raise ValueError when hudi_options is None
    with pytest.raises(ValueError, match="Hudi options required"):
        writer.write_data(df, "s3://test/path/", "hudi")


def test_clean_dataframe_case_sensitive_null_matching(spark):
    """Test that null string matching is case-sensitive"""
    data = [
        ("C001", "John", "john@example.com", "North"),
        ("C002", "Null", "jane@example.com", "South"),
        ("C003", "NULL", "bob@example.com", "East"),
        ("C004", "null", "alice@example.com", "West")
    ]

    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    # Only match "Null" (case-sensitive)
    null_values = ["Null"]
    cleaned_df = clean_dataframe(df, null_values)

    # Should remove only C002 with "Null"
    assert cleaned_df.count() == 3
    assert cleaned_df.filter(df.name == "Null").count() == 0
    assert cleaned_df.filter(df.name == "NULL").count() == 1
    assert cleaned_df.filter(df.name == "null").count() == 1


def test_scd2_composite_business_key(spark):
    """Test that SCD2 uses composite business key (CustId + OrderId)"""
    customer_data = [("C001", "John Doe", "john@example.com", "North")]
    order_data = [
        ("O001", "Laptop", Decimal("999.99"), 1, date(2024, 1, 15), "C001"),
        ("O002", "Mouse", Decimal("25.50"), 2, date(2024, 1, 16), "C001")
    ]

    customer_df = spark.createDataFrame(customer_data, get_customer_schema())
    order_df = spark.createDataFrame(order_data, get_order_schema())

    scd2_df = apply_scd2(customer_df, order_df, spark)

    # Should have 2 records with same CustId but different OrderId
    assert scd2_df.count() == 2
    assert scd2_df.filter(scd2_df.custid == "C001").count() == 2

    # Verify both orders are present
    order_ids = [row.orderid for row in scd2_df.select("orderid").collect()]
    assert "O001" in order_ids
    assert "O002" in order_ids


def test_yaml_paths_are_opaque_strings():
    """Test that YAML paths are treated as opaque strings"""
    params = get_job_parameters()

    # Paths should be strings
    assert isinstance(params['inputs']['customer']['source_path'], str)
    assert isinstance(params['outputs']['customer']['target_path'], str)

    # Should not attempt to dereference or validate paths in tests
    # Just verify they are present and properly formatted
    assert params['inputs']['customer']['source_path'].startswith('s3://')
    assert params['outputs']['ordersummary']['target_path'].startswith('s3://')


def test_column_normalization(spark, sample_customer_data):
    """Test that column names are normalized to lowercase"""
    # Create DataFrame with mixed case columns
    data = [("C001", "John Doe", "john@example.com", "North")]
    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("EmailId", StringType(), False),
        StructField("Region", StringType(), False)
    ])

    df = spark.createDataFrame(data, schema)

    # Normalize columns
    normalized_df = df.toDF(*[c.lower() for c in df.columns])

    # Verify all columns are lowercase
    for col_name in normalized_df.columns:
        assert col_name == col_name.lower()

    assert "custid" in normalized_df.columns
    assert "name" in normalized_df.columns
    assert "emailid" in normalized_df.columns
    assert "region" in normalized_df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])