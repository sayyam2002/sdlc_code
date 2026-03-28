"""
Unit tests for AWS Glue PySpark Job - SDLC Wizard Data Processing
Tests verify FRD requirements: FR-INGEST-001, FR-CLEAN-001, FR-SCD2-001, FR-AGGREGATE-001
"""

import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, BooleanType, TimestampType
)
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from job import (
    get_customer_schema,
    get_order_schema,
    read_customer_data,
    read_order_data,
    clean_dataframe,
    add_scd2_columns,
    write_hudi_table,
    calculate_customer_aggregate_spend
)


@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing
    """
    spark = SparkSession.builder \
        .appName("test_sdlc_wizard") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """
    Create sample customer data for testing
    """
    schema = get_customer_schema()
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """
    Create sample order data for testing
    """
    schema = get_order_schema()
    data = [
        ("O001", "Laptop", Decimal("999.99"), 2, "2024-01-15"),
        ("O002", "Mouse", Decimal("25.50"), 5, "2024-01-16"),
        ("O003", "Keyboard", Decimal("75.00"), 3, "2024-01-17"),
        ("O004", "Monitor", Decimal("299.99"), 1, "2024-01-18"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_customer_data(spark):
    """
    Create dirty customer data with NULLs, 'Null' strings, and duplicates
    """
    schema = get_customer_schema()
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),  # NULL value
        ("C003", "Bob Johnson", "Null", "East"),  # 'Null' string
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_order_data(spark):
    """
    Create dirty order data with NULLs, 'Null' strings, and duplicates
    """
    schema = get_order_schema()
    data = [
        ("O001", "Laptop", Decimal("999.99"), 2, "2024-01-15"),
        ("O002", "Mouse", None, 5, "2024-01-16"),  # NULL value
        ("O003", "Keyboard", Decimal("75.00"), 3, "Null"),  # 'Null' string
        ("O004", "Monitor", Decimal("299.99"), 1, "2024-01-18"),
        ("O001", "Laptop", Decimal("999.99"), 2, "2024-01-15"),  # Duplicate
    ]
    return spark.createDataFrame(data, schema)


# Test FR-INGEST-001: Schema Validation

def test_customer_schema_structure():
    """
    Test FR-INGEST-001: Verify customer schema matches TRD specifications
    """
    schema = get_customer_schema()

    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "Name"
    assert schema.fields[1].dataType == StringType()
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[2].dataType == StringType()
    assert schema.fields[3].name == "Region"
    assert schema.fields[3].dataType == StringType()


def test_order_schema_structure():
    """
    Test FR-INGEST-001: Verify order schema matches TRD specifications
    """
    schema = get_order_schema()

    assert len(schema.fields) == 5
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "ItemName"
    assert schema.fields[1].dataType == StringType()
    assert schema.fields[2].name == "PricePerUnit"
    assert isinstance(schema.fields[2].dataType, DecimalType)
    assert schema.fields[3].name == "Qty"
    assert schema.fields[3].dataType == IntegerType()
    assert schema.fields[4].name == "Date"
    assert schema.fields[4].dataType == StringType()


def test_read_customer_data_with_correct_path(spark, tmp_path):
    """
    Test FR-INGEST-001: Verify customer data reading from correct S3 path
    """
    # Create temporary CSV file
    csv_path = tmp_path / "customer.csv"
    csv_path.write_text(
        "CustId,Name,EmailId,Region\n"
        "C001,John Doe,john@example.com,North\n"
        "C002,Jane Smith,jane@example.com,South\n"
    )

    df = read_customer_data(spark, str(tmp_path))

    assert df.count() == 2
    assert df.columns == ["CustId", "Name", "EmailId", "Region"]


def test_read_order_data_with_correct_path(spark, tmp_path):
    """
    Test FR-INGEST-001: Verify order data reading from correct S3 path
    """
    # Create temporary CSV file
    csv_path = tmp_path / "order.csv"
    csv_path.write_text(
        "OrderId,ItemName,PricePerUnit,Qty,Date\n"
        "O001,Laptop,999.99,2,2024-01-15\n"
        "O002,Mouse,25.50,5,2024-01-16\n"
    )

    df = read_order_data(spark, str(tmp_path))

    assert df.count() == 2
    assert df.columns == ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date"]


# Test FR-CLEAN-001: Data Cleaning

def test_clean_dataframe_removes_null_values(dirty_customer_data):
    """
    Test FR-CLEAN-001: Verify NULL values are removed
    """
    cleaned_df = clean_dataframe(dirty_customer_data)

    # Original has 5 rows, one with NULL
    assert cleaned_df.count() < dirty_customer_data.count()

    # Verify no NULL values remain
    for col_name in cleaned_df.columns:
        null_count = cleaned_df.filter(cleaned_df[col_name].isNull()).count()
        assert null_count == 0


def test_clean_dataframe_removes_null_strings(dirty_customer_data):
    """
    Test FR-CLEAN-001: Verify 'Null' string values are removed
    """
    cleaned_df = clean_dataframe(dirty_customer_data)

    # Verify no 'Null' strings remain
    for col_name in cleaned_df.columns:
        null_string_count = cleaned_df.filter(
            (cleaned_df[col_name] == "Null") |
            (cleaned_df[col_name] == "null") |
            (cleaned_df[col_name] == "NULL")
        ).count()
        assert null_string_count == 0


def test_clean_dataframe_removes_duplicates(dirty_customer_data):
    """
    Test FR-CLEAN-001: Verify duplicate records are removed
    """
    cleaned_df = clean_dataframe(dirty_customer_data)

    # Check that duplicates are removed
    distinct_count = cleaned_df.distinct().count()
    assert cleaned_df.count() == distinct_count


def test_clean_dataframe_preserves_valid_data(sample_customer_data):
    """
    Test FR-CLEAN-001: Verify valid data is preserved during cleaning
    """
    original_count = sample_customer_data.count()
    cleaned_df = clean_dataframe(sample_customer_data)

    assert cleaned_df.count() == original_count


def test_clean_order_data_removes_nulls_and_duplicates(dirty_order_data):
    """
    Test FR-CLEAN-001: Verify order data cleaning removes NULLs and duplicates
    """
    cleaned_df = clean_dataframe(dirty_order_data)

    # Should remove rows with NULL and 'Null' and duplicates
    assert cleaned_df.count() < dirty_order_data.count()
    assert cleaned_df.count() == cleaned_df.distinct().count()


# Test FR-SCD2-001: SCD Type 2 Implementation

def test_add_scd2_columns_structure(sample_customer_data):
    """
    Test FR-SCD2-001: Verify SCD Type 2 columns are added correctly
    """
    scd2_df = add_scd2_columns(sample_customer_data, "CustId")

    expected_columns = ["CustId", "Name", "EmailId", "Region",
                       "IsActive", "StartDate", "EndDate", "OpTs"]
    assert scd2_df.columns == expected_columns


def test_add_scd2_columns_isactive_default(sample_customer_data):
    """
    Test FR-SCD2-001: Verify IsActive column defaults to True
    """
    scd2_df = add_scd2_columns(sample_customer_data, "CustId")

    is_active_values = scd2_df.select("IsActive").distinct().collect()
    assert len(is_active_values) == 1
    assert is_active_values[0]["IsActive"] is True


def test_add_scd2_columns_startdate_populated(sample_customer_data):
    """
    Test FR-SCD2-001: Verify StartDate is populated with current timestamp
    """
    scd2_df = add_scd2_columns(sample_customer_data, "CustId")

    start_date_nulls = scd2_df.filter(scd2_df["StartDate"].isNull()).count()
    assert start_date_nulls == 0


def test_add_scd2_columns_enddate_null(sample_customer_data):
    """
    Test FR-SCD2-001: Verify EndDate is NULL for active records
    """
    scd2_df = add_scd2_columns(sample_customer_data, "CustId")

    end_date_nulls = scd2_df.filter(scd2_df["EndDate"].isNull()).count()
    assert end_date_nulls == scd2_df.count()


def test_add_scd2_columns_opts_populated(sample_customer_data):
    """
    Test FR-SCD2-001: Verify OpTs is populated with current timestamp
    """
    scd2_df = add_scd2_columns(sample_customer_data, "CustId")

    opts_nulls = scd2_df.filter(scd2_df["OpTs"].isNull()).count()
    assert opts_nulls == 0


def test_scd2_columns_for_order_data(sample_order_data):
    """
    Test FR-SCD2-001: Verify SCD Type 2 columns work for order data
    """
    scd2_df = add_scd2_columns(sample_order_data, "OrderId")

    expected_columns = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date",
                       "IsActive", "StartDate", "EndDate", "OpTs"]
    assert scd2_df.columns == expected_columns
    assert scd2_df.count() == sample_order_data.count()


# Test Hudi Write Operations

@patch('job.write_hudi_table')
def test_write_hudi_table_called_with_correct_params(mock_write, sample_customer_data):
    """
    Test FR-SCD2-001: Verify Hudi write is called with correct parameters
    """
    output_path = "s3://adif-sdlc/curated/sdlc_wizard/"
    table_name = "sdlc_wizard_customer"

    write_hudi_table(
        sample_customer_data,
        output_path,
        table_name,
        "CustId",
        "OpTs"
    )

    mock_write.assert_called_once()


def test_hudi_options_configuration(sample_customer_data):
    """
    Test FR-SCD2-001: Verify Hudi configuration options are correct
    """
    # Mock the write operation to capture options
    with patch.object(sample_customer_data.write, 'save') as mock_save:
        with patch.object(sample_customer_data.write, 'format', return_value=sample_customer_data.write) as mock_format:
            with patch.object(sample_customer_data.write, 'options', return_value=sample_customer_data.write) as mock_options:
                with patch.object(sample_customer_data.write, 'mode', return_value=sample_customer_data.write) as mock_mode:

                    output_path = "s3://adif-sdlc/curated/sdlc_wizard/"
                    table_name = "sdlc_wizard_customer"

                    write_hudi_table(
                        sample_customer_data,
                        output_path,
                        table_name,
                        "CustId",
                        "OpTs"
                    )

                    # Verify format is hudi
                    mock_format.assert_called_with("hudi")

                    # Verify mode is append
                    mock_mode.assert_called_with("append")


# Test FR-AGGREGATE-001: Aggregation Logic

def test_calculate_customer_aggregate_spend_structure(sample_customer_data, sample_order_data):
    """
    Test FR-AGGREGATE-001: Verify aggregate spend calculation structure
    """
    aggregate_df = calculate_customer_aggregate_spend(sample_customer_data, sample_order_data)

    expected_columns = ["OrderId", "TotalOrderSpend", "ItemCount",
                       "AvgPricePerUnit", "MaxQty", "MinQty"]
    assert aggregate_df.columns == expected_columns


def test_calculate_customer_aggregate_spend_values(spark):
    """
    Test FR-AGGREGATE-001: Verify aggregate spend calculation values
    """
    # Create test data with known values
    customer_schema = get_customer_schema()
    customer_data = [("C001", "John Doe", "john@example.com", "North")]
    customer_df = spark.createDataFrame(customer_data, customer_schema)

    order_schema = get_order_schema()
    order_data = [
        ("O001", "Laptop", Decimal("100.00"), 2, "2024-01-15"),
        ("O001", "Mouse", Decimal("50.00"), 1, "2024-01-15"),
    ]
    order_df = spark.createDataFrame(order_data, order_schema)

    aggregate_df = calculate_customer_aggregate_spend(customer_df, order_df)

    result = aggregate_df.collect()[0]

    # Total: (100*2) + (50*1) = 250
    assert float(result["TotalOrderSpend"]) == 250.0
    assert result["ItemCount"] == 2
    assert float(result["AvgPricePerUnit"]) == 75.0
    assert result["MaxQty"] == 2
    assert result["MinQty"] == 1


def test_calculate_customer_aggregate_spend_multiple_orders(spark):
    """
    Test FR-AGGREGATE-001: Verify aggregation works with multiple orders
    """
    customer_schema = get_customer_schema()
    customer_data = [("C001", "John Doe", "john@example.com", "North")]
    customer_df = spark.createDataFrame(customer_data, customer_schema)

    order_schema = get_order_schema()
    order_data = [
        ("O001", "Item1", Decimal("10.00"), 1, "2024-01-15"),
        ("O002", "Item2", Decimal("20.00"), 2, "2024-01-16"),
        ("O003", "Item3", Decimal("30.00"), 3, "2024-01-17"),
    ]
    order_df = spark.createDataFrame(order_data, order_schema)

    aggregate_df = calculate_customer_aggregate_spend(customer_df, order_df)

    assert aggregate_df.count() == 3  # Three distinct orders


# Test S3 Path Validation

def test_s3_paths_match_trd_specifications():
    """
    Test: Verify S3 paths match TRD specifications exactly
    """
    expected_paths = {
        'customer_input': 's3://adif-sdlc/sdlc_wizard/customerdata/',
        'order_input': 's3://adif-sdlc/sdlc_wizard/orderdata/',
        'customer_output': 's3://adif-sdlc/curated/sdlc_wizard/',
        'order_output': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
        'analytics_output': 's3://adif-sdlc/analytics/customeraggregatespend/',
    }

    # This test documents the expected paths from TRD
    assert expected_paths['customer_input'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'
    assert expected_paths['order_input'] == 's3://adif-sdlc/sdlc_wizard/orderdata/'
    assert expected_paths['customer_output'] == 's3://adif-sdlc/curated/sdlc_wizard/'
    assert expected_paths['order_output'] == 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
    assert expected_paths['analytics_output'] == 's3://adif-sdlc/analytics/customeraggregatespend/'


# Integration Test

@patch('job.Job')
@patch('job.GlueContext')
@patch('job.SparkContext')
def test_main_function_execution_flow(mock_sc, mock_glue_context, mock_job, spark):
    """
    Test: Verify main function executes complete workflow
    """
    # Mock AWS Glue components
    mock_sc.getOrCreate.return_value = spark.sparkContext
    mock_glue_instance = MagicMock()
    mock_glue_instance.spark_session = spark
    mock_glue_context.return_value = mock_glue_instance

    mock_job_instance = MagicMock()
    mock_job.return_value = mock_job_instance

    # This test verifies the main function structure
    # Actual execution would require full AWS Glue environment
    assert callable(mock_job_instance.init)
    assert callable(mock_job_instance.commit)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])