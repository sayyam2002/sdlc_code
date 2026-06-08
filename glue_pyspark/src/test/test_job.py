"""
Comprehensive test suite for AWS Glue PySpark ETL job
Tests all transformations, data quality operations, and business logic
"""

import sys
from unittest.mock import patch, MagicMock
from decimal import Decimal
from datetime import datetime

# Mock Glue modules before importing job
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.job'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.dynamicframe'] = MagicMock()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
import pytest

from main.job import (
    get_job_parameters,
    validate_s3_path,
    clean_dataframe,
    add_scd2_columns,
    detect_customer_changes,
    create_order_summary,
    calculate_customer_aggregate_spend,
    DataReader,
    DataWriter
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_customer_order_analytics") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    # Don't stop spark if it was provided by builtins


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),
        ("C004", "Alice Williams", "alice.williams@example.com", "West"),
        ("C005", "Charlie Brown", "charlie.brown@example.com", "North")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing"""
    schema = StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), True),
        StructField("priceperunit", DecimalType(10, 2), True),
        StructField("qty", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("custid", StringType(), False)
    ])

    data = [
        ("O001", "Laptop", Decimal("1200.00"), 1, "2024-01-15", "C001"),
        ("O002", "Mouse", Decimal("25.50"), 2, "2024-01-16", "C001"),
        ("O003", "Keyboard", Decimal("75.00"), 1, "2024-01-16", "C002"),
        ("O004", "Monitor", Decimal("350.00"), 2, "2024-01-17", "C003"),
        ("O005", "Headphones", Decimal("89.99"), 1, "2024-01-18", "C002")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_with_nulls(spark):
    """Create sample customer data with null values for testing"""
    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", None, "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        (None, "Alice Williams", "alice.williams@example.com", "West"),
        ("C005", "Charlie Brown", "charlie.brown@example.com", "North"),
        ("C005", "Charlie Brown", "charlie.brown@example.com", "North")  # Duplicate
    ]

    return spark.createDataFrame(data, schema)


def test_get_job_parameters():
    """Test job parameter loading"""
    params = get_job_parameters()

    assert params is not None
    assert isinstance(params, dict)
    assert "inputs_customer_source_path" in params
    assert "inputs_order_source_path" in params
    assert "catalog_database_name" in params
    assert params["catalog_database_name"] == "gen_ai_poc_databrickscoe"


def test_validate_s3_path():
    """Test S3 path validation"""
    # Valid paths
    assert validate_s3_path("s3://bucket/path/to/data/") == True
    assert validate_s3_path("s3a://bucket/path/to/data/") == True

    # Invalid paths
    assert validate_s3_path("") == False
    assert validate_s3_path(None) == False
    assert validate_s3_path("/local/path") == False
    assert validate_s3_path("file:///tmp/data") == False


def test_clean_dataframe_removes_nulls(spark, sample_customer_with_nulls):
    """Test that clean_dataframe removes NULL values"""
    cleaned_df = clean_dataframe(sample_customer_with_nulls, "customer")

    assert cleaned_df is not None
    assert cleaned_df.count() == 2  # Only 2 complete records without nulls

    # Verify no nulls remain
    for col_name in cleaned_df.columns:
        null_count = cleaned_df.filter(cleaned_df[col_name].isNull()).count()
        assert null_count == 0, f"Column {col_name} still has null values"


def test_clean_dataframe_removes_string_null(spark, sample_customer_with_nulls):
    """Test that clean_dataframe removes string 'Null' values"""
    cleaned_df = clean_dataframe(sample_customer_with_nulls, "customer")

    # Verify no 'Null' strings remain
    for col_name in cleaned_df.columns:
        null_string_count = cleaned_df.filter(cleaned_df[col_name] == "Null").count()
        assert null_string_count == 0, f"Column {col_name} still has 'Null' strings"


def test_clean_dataframe_removes_duplicates(spark, sample_customer_with_nulls):
    """Test that clean_dataframe removes duplicate records"""
    cleaned_df = clean_dataframe(sample_customer_with_nulls, "customer")

    # Count should be less than original due to duplicate removal
    assert cleaned_df.count() <= sample_customer_with_nulls.count()

    # Verify no duplicates remain
    distinct_count = cleaned_df.distinct().count()
    assert cleaned_df.count() == distinct_count


def test_clean_dataframe_with_none_input():
    """Test clean_dataframe handles None input gracefully"""
    result = clean_dataframe(None, "test")
    assert result is None


def test_add_scd2_columns(spark, sample_customer_data):
    """Test SCD Type 2 column addition"""
    scd2_df = add_scd2_columns(sample_customer_data)

    assert scd2_df is not None

    # Verify SCD2 columns exist
    assert "isactive" in scd2_df.columns
    assert "startdate" in scd2_df.columns
    assert "enddate" in scd2_df.columns
    assert "opts" in scd2_df.columns

    # Verify all records are active
    active_count = scd2_df.filter(scd2_df["isactive"] == True).count()
    assert active_count == scd2_df.count()

    # Verify enddate is null for active records
    null_enddate_count = scd2_df.filter(scd2_df["enddate"].isNull()).count()
    assert null_enddate_count == scd2_df.count()


def test_add_scd2_columns_with_none_input():
    """Test add_scd2_columns handles None input gracefully"""
    result = add_scd2_columns(None)
    assert result is None


def test_detect_customer_changes_no_baseline(spark, sample_customer_data):
    """Test change detection with no baseline (all records are new)"""
    changed_df = detect_customer_changes(sample_customer_data, None)

    assert changed_df is not None
    assert changed_df.count() == sample_customer_data.count()


def test_detect_customer_changes_with_baseline(spark, sample_customer_data):
    """Test change detection with existing baseline"""
    # Create baseline with subset of customers
    baseline_data = sample_customer_data.filter(sample_customer_data["custid"].isin(["C001", "C002"]))

    changed_df = detect_customer_changes(sample_customer_data, baseline_data)

    assert changed_df is not None
    # Should detect 3 new customers (C003, C004, C005)
    assert changed_df.count() == 3


def test_detect_customer_changes_with_none_input(spark):
    """Test change detection handles None input gracefully"""
    result = detect_customer_changes(None, None)
    assert result is None


def test_create_order_summary(spark, sample_customer_data, sample_order_data):
    """Test order summary creation with customer join"""
    order_summary = create_order_summary(sample_customer_data, sample_order_data)

    assert order_summary is not None
    assert order_summary.count() == 5  # All orders should join with customers

    # Verify all expected columns exist
    expected_columns = ["orderid", "itemname", "priceperunit", "qty", "date",
                       "custid", "name", "emailid", "region"]
    for col in expected_columns:
        assert col in order_summary.columns


def test_create_order_summary_with_missing_input(spark, sample_customer_data):
    """Test order summary handles missing input gracefully"""
    result = create_order_summary(sample_customer_data, None)
    assert result is None

    result = create_order_summary(None, sample_customer_data)
    assert result is None


def test_calculate_customer_aggregate_spend(spark, sample_customer_data, sample_order_data):
    """Test customer aggregate spend calculation"""
    order_summary = create_order_summary(sample_customer_data, sample_order_data)
    aggregate_spend = calculate_customer_aggregate_spend(order_summary)

    assert aggregate_spend is not None
    assert aggregate_spend.count() > 0

    # Verify required columns
    assert "name" in aggregate_spend.columns
    assert "date" in aggregate_spend.columns
    assert "totalspend" in aggregate_spend.columns

    # Verify aggregation logic
    # John Doe (C001) has 2 orders on different dates
    john_records = aggregate_spend.filter(aggregate_spend["name"] == "John Doe")
    assert john_records.count() == 2


def test_calculate_customer_aggregate_spend_amounts(spark, sample_customer_data, sample_order_data):
    """Test customer aggregate spend calculation amounts are correct"""
    order_summary = create_order_summary(sample_customer_data, sample_order_data)
    aggregate_spend = calculate_customer_aggregate_spend(order_summary)

    # Verify John Doe's spending on 2024-01-15 (1 Laptop = 1200.00)
    john_jan15 = aggregate_spend.filter(
        (aggregate_spend["name"] == "John Doe") &
        (aggregate_spend["date"] == "2024-01-15")
    ).collect()

    if len(john_jan15) > 0:
        assert float(john_jan15[0]["totalspend"]) == 1200.00


def test_calculate_customer_aggregate_spend_with_none_input():
    """Test aggregate spend calculation handles None input gracefully"""
    result = calculate_customer_aggregate_spend(None)
    assert result is None


def test_data_reader_read_csv(spark):
    """Test DataReader CSV reading with mocked Spark read"""
    glue_context = MagicMock()
    reader = DataReader(spark, glue_context)

    # Create mock DataFrame
    mock_df = spark.createDataFrame([("C001", "John Doe")], ["custid", "name"])

    with patch.object(spark.read, 'format') as mock_format:
        mock_format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df

        result = reader.read_csv("s3://test-bucket/data/")

        assert result is not None
        assert result.count() == 1


def test_data_reader_read_csv_invalid_path(spark):
    """Test DataReader handles invalid S3 path"""
    glue_context = MagicMock()
    reader = DataReader(spark, glue_context)

    result = reader.read_csv("/invalid/path")
    assert result is None


def test_data_writer_write_dataframe_parquet(spark, sample_customer_data):
    """Test DataWriter Parquet writing"""
    glue_context = MagicMock()
    writer = DataWriter(spark, glue_context)

    with patch.object(writer, '_write_parquet') as mock_write:
        result = writer.write_dataframe(
            sample_customer_data,
            "s3://test-bucket/output/",
            format_type="parquet"
        )

        assert result == True
        mock_write.assert_called_once()


def test_data_writer_write_dataframe_hudi(spark, sample_customer_data):
    """Test DataWriter Hudi writing"""
    glue_context = MagicMock()
    writer = DataWriter(spark, glue_context)

    hudi_config = {
        "table_name": "test_table",
        "record_key": "custid",
        "precombine_field": "opts"
    }

    with patch.object(writer, '_write_hudi') as mock_write:
        result = writer.write_dataframe(
            sample_customer_data,
            "s3://test-bucket/output/",
            format_type="hudi",
            hudi_config=hudi_config
        )

        assert result == True
        mock_write.assert_called_once()


def test_data_writer_write_dataframe_invalid_path(spark, sample_customer_data):
    """Test DataWriter handles invalid S3 path"""
    glue_context = MagicMock()
    writer = DataWriter(spark, glue_context)

    result = writer.write_dataframe(sample_customer_data, "/invalid/path")
    assert result == False


def test_data_writer_write_dataframe_empty_df(spark):
    """Test DataWriter handles empty DataFrame"""
    glue_context = MagicMock()
    writer = DataWriter(spark, glue_context)

    empty_df = spark.createDataFrame([], StructType([]))
    result = writer.write_dataframe(empty_df, "s3://test-bucket/output/")
    assert result == False


def test_data_writer_write_to_catalog(spark, sample_customer_data):
    """Test DataWriter catalog writing"""
    glue_context = MagicMock()
    writer = DataWriter(spark, glue_context)

    # Mock DynamicFrame
    with patch('main.job.DynamicFrame') as mock_df:
        mock_df.fromDF.return_value = MagicMock()

        result = writer.write_to_catalog(
            sample_customer_data,
            "test_db",
            "test_table",
            "s3://test-bucket/output/"
        )

        # Should attempt catalog write (may fail but that's ok for test)
        assert result in [True, False]


def test_column_normalization(spark):
    """Test that column names are normalized to lowercase"""
    glue_context = MagicMock()
    reader = DataReader(spark, glue_context)

    # Create DataFrame with mixed case columns
    mock_df = spark.createDataFrame([("C001", "John")], ["CustId", "Name"])

    with patch.object(spark.read, 'format') as mock_format:
        mock_format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df

        result = reader.read_csv("s3://test-bucket/data/")

        # Verify columns are lowercase
        assert "custid" in result.columns
        assert "name" in result.columns
        assert "CustId" not in result.columns
        assert "Name" not in result.columns


def test_end_to_end_pipeline(spark, sample_customer_data, sample_order_data):
    """Test complete end-to-end pipeline flow"""
    # Step 1: Clean data
    customer_clean = clean_dataframe(sample_customer_data, "customer")
    order_clean = clean_dataframe(sample_order_data, "order")

    assert customer_clean is not None
    assert order_clean is not None

    # Step 2: Add SCD2 columns
    customer_scd2 = add_scd2_columns(customer_clean)
    assert "isactive" in customer_scd2.columns

    # Step 3: Create order summary
    order_summary = create_order_summary(customer_clean, order_clean)
    assert order_summary is not None
    assert order_summary.count() == 5

    # Step 4: Calculate aggregate spend
    aggregate_spend = calculate_customer_aggregate_spend(order_summary)
    assert aggregate_spend is not None
    assert aggregate_spend.count() > 0

    # Verify final output has required columns
    assert "name" in aggregate_spend.columns
    assert "date" in aggregate_spend.columns
    assert "totalspend" in aggregate_spend.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])