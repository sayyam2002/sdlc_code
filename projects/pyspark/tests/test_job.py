"""
Comprehensive test suite for Customer Order Data Processing Pipeline
Tests all FRD requirements: FR-INGEST-001, FR-CLEAN-001, FR-SCD2-001, FR-AGG-001
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    BooleanType, TimestampType
)
from decimal import Decimal
from datetime import datetime
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from job import CustomerOrderProcessor


@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing
    """
    spark = SparkSession.builder \
        .appName("CustomerOrderProcessorTest") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def processor(spark):
    """
    Create a CustomerOrderProcessor instance for testing
    """
    return CustomerOrderProcessor(spark_session=spark)


@pytest.fixture
def sample_customer_data(spark):
    """
    Create sample customer data matching TRD schema
    """
    data = [
        ("C001", "John Doe"),
        ("C002", "Jane Smith"),
        ("C003", "Bob Johnson"),
        ("C004", "Alice Williams"),
        ("C005", "Charlie Brown")
    ]

    schema = StructType([
        StructField("Customer", StringType(), True),
        StructField("name", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """
    Create sample order data matching TRD schema
    """
    data = [
        ("C001", "Order-001", Decimal("100.50")),
        ("C001", "Order-002", Decimal("200.75")),
        ("C002", "Order-003", Decimal("150.00")),
        ("C003", "Order-004", Decimal("300.25")),
        ("C002", "Order-005", Decimal("50.50"))
    ]

    schema = StructType([
        StructField("Customer", StringType(), True),
        StructField("ordersummary", StringType(), True),
        StructField("TotalAmount", DecimalType(10, 2), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_customer_data(spark):
    """
    Create customer data with NULL and 'Null' values for cleaning tests
    """
    data = [
        ("C001", "John Doe"),
        ("C002", None),  # NULL value
        ("C003", "Null"),  # 'Null' string
        (None, "Jane Smith"),  # NULL Customer
        ("C004", "Alice Williams"),
        ("C005", "NULL"),  # 'NULL' string
        ("C004", "Alice Williams")  # Duplicate
    ]

    schema = StructType([
        StructField("Customer", StringType(), True),
        StructField("name", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_order_data(spark):
    """
    Create order data with NULL values and duplicates
    """
    data = [
        ("C001", "Order-001", Decimal("100.50")),
        ("C001", None, Decimal("200.75")),  # NULL ordersummary
        ("C002", "Order-003", None),  # NULL TotalAmount
        (None, "Order-004", Decimal("300.25")),  # NULL Customer
        ("C001", "Order-001", Decimal("100.50")),  # Duplicate
        ("C003", "Null", Decimal("150.00"))  # 'Null' string
    ]

    schema = StructType([
        StructField("Customer", StringType(), True),
        StructField("ordersummary", StringType(), True),
        StructField("TotalAmount", DecimalType(10, 2), True)
    ])

    return spark.createDataFrame(data, schema)


# Test FR-INGEST-001: Data Ingestion
def test_customer_schema_definition(processor):
    """
    Test that customer schema matches TRD specification
    """
    schema = processor.get_customer_schema()

    assert len(schema.fields) == 2
    assert schema.fields[0].name == "Customer"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "name"
    assert schema.fields[1].dataType == StringType()


def test_order_schema_definition(processor):
    """
    Test that order schema matches TRD specification
    """
    schema = processor.get_order_schema()

    assert len(schema.fields) == 3
    assert schema.fields[0].name == "Customer"
    assert schema.fields[1].name == "ordersummary"
    assert schema.fields[2].name == "TotalAmount"
    assert isinstance(schema.fields[2].dataType, DecimalType)


def test_s3_paths_match_trd(processor):
    """
    Test that S3 paths match TRD specifications
    """
    assert processor.order_source_path == "s3://adif-sdlc/sdlc_wizard/orderdata/"
    assert processor.customer_source_path == "s3://adif-sdlc/catalog/sdlc_wizard/customer/"
    assert processor.output_path == "s3://adif-sdlc/analytics/customeraggregatespend/"


def test_catalog_database_name(processor):
    """
    Test that Glue Catalog database name matches TRD
    """
    assert processor.catalog_database == "sdlc_wizard"


# Test FR-CLEAN-001 / TR-CLEAN-001: Data Cleaning
def test_clean_data_removes_nulls(processor, dirty_customer_data):
    """
    Test that clean_data removes rows with NULL values
    """
    cleaned_df = processor.clean_data(dirty_customer_data)

    # Should only have rows without NULLs
    assert cleaned_df.count() == 2  # C001 and C004 (before duplicate removal)

    # Verify no NULL values remain
    null_count = cleaned_df.filter(
        cleaned_df.Customer.isNull() | cleaned_df.name.isNull()
    ).count()
    assert null_count == 0


def test_clean_data_removes_null_strings(processor, dirty_customer_data):
    """
    Test that clean_data removes rows with 'Null' string values
    """
    cleaned_df = processor.clean_data(dirty_customer_data)

    # Verify no 'Null' strings remain
    null_string_count = cleaned_df.filter(
        (cleaned_df.name == "Null") |
        (cleaned_df.name == "NULL") |
        (cleaned_df.name == "null")
    ).count()
    assert null_string_count == 0


def test_clean_data_removes_duplicates(processor, dirty_customer_data):
    """
    Test that clean_data removes duplicate records
    """
    cleaned_df = processor.clean_data(dirty_customer_data)

    # After removing NULLs and 'Null' strings, C004 appears twice
    # Should be deduplicated to 1
    c004_count = cleaned_df.filter(cleaned_df.Customer == "C004").count()
    assert c004_count == 1


def test_clean_order_data(processor, dirty_order_data):
    """
    Test cleaning of order data with NULLs and duplicates
    """
    cleaned_df = processor.clean_data(dirty_order_data)

    # Should only have valid, non-duplicate records
    assert cleaned_df.count() == 1  # Only C001/Order-001 after dedup

    # Verify no NULLs
    null_count = cleaned_df.filter(
        cleaned_df.Customer.isNull() |
        cleaned_df.ordersummary.isNull() |
        cleaned_df.TotalAmount.isNull()
    ).count()
    assert null_count == 0


def test_clean_data_preserves_valid_records(processor, sample_customer_data):
    """
    Test that clean_data preserves all valid records
    """
    cleaned_df = processor.clean_data(sample_customer_data)

    # All sample records are valid, should be preserved
    assert cleaned_df.count() == sample_customer_data.count()


# Test FR-AGG-001: Aggregation
def test_aggregate_customer_spending(processor, sample_order_data, sample_customer_data):
    """
    Test customer spending aggregation logic
    """
    aggregated_df = processor.aggregate_customer_spending(
        sample_order_data,
        sample_customer_data
    )

    # Should have 3 customers with orders
    assert aggregated_df.count() == 3

    # Verify aggregation for C001
    c001_row = aggregated_df.filter(aggregated_df.Customer == "C001").collect()[0]
    assert c001_row["TotalAmount"] == Decimal("301.25")  # 100.50 + 200.75
    assert c001_row["name"] == "John Doe"

    # Verify aggregation for C002
    c002_row = aggregated_df.filter(aggregated_df.Customer == "C002").collect()[0]
    assert c002_row["TotalAmount"] == Decimal("200.50")  # 150.00 + 50.50
    assert c002_row["name"] == "Jane Smith"


def test_aggregate_includes_customer_name(processor, sample_order_data, sample_customer_data):
    """
    Test that aggregation includes customer name from join
    """
    aggregated_df = processor.aggregate_customer_spending(
        sample_order_data,
        sample_customer_data
    )

    # Verify schema includes name column
    assert "name" in aggregated_df.columns
    assert "Customer" in aggregated_df.columns
    assert "TotalAmount" in aggregated_df.columns


def test_aggregate_handles_single_order(processor, spark, sample_customer_data):
    """
    Test aggregation with customers having single orders
    """
    single_order_data = [
        ("C003", "Order-001", Decimal("100.00"))
    ]

    schema = StructType([
        StructField("Customer", StringType(), True),
        StructField("ordersummary", StringType(), True),
        StructField("TotalAmount", DecimalType(10, 2), True)
    ])

    order_df = spark.createDataFrame(single_order_data, schema)

    aggregated_df = processor.aggregate_customer_spending(
        order_df,
        sample_customer_data
    )

    assert aggregated_df.count() == 1
    row = aggregated_df.collect()[0]
    assert row["TotalAmount"] == Decimal("100.00")


# Test FR-SCD2-001: SCD Type 2
def test_add_scd2_columns_initial_load(processor, sample_customer_data):
    """
    Test that SCD Type 2 columns are added correctly for initial load
    """
    scd2_df = processor.add_scd2_columns(sample_customer_data, is_initial_load=True)

    # Verify all SCD2 columns exist
    assert "IsActive" in scd2_df.columns
    assert "StartDate" in scd2_df.columns
    assert "EndDate" in scd2_df.columns
    assert "OpTs" in scd2_df.columns

    # Verify all records are active
    active_count = scd2_df.filter(scd2_df.IsActive == True).count()
    assert active_count == sample_customer_data.count()

    # Verify EndDate is NULL for active records
    null_end_date_count = scd2_df.filter(scd2_df.EndDate.isNull()).count()
    assert null_end_date_count == sample_customer_data.count()


def test_scd2_columns_data_types(processor, sample_customer_data):
    """
    Test that SCD Type 2 columns have correct data types
    """
    scd2_df = processor.add_scd2_columns(sample_customer_data)

    schema_dict = {field.name: field.dataType for field in scd2_df.schema.fields}

    assert isinstance(schema_dict["IsActive"], BooleanType)
    assert isinstance(schema_dict["StartDate"], TimestampType)
    assert isinstance(schema_dict["EndDate"], TimestampType)
    assert isinstance(schema_dict["OpTs"], TimestampType)


def test_scd2_preserves_original_columns(processor, sample_customer_data):
    """
    Test that SCD Type 2 transformation preserves original columns
    """
    scd2_df = processor.add_scd2_columns(sample_customer_data)

    # Original columns should still exist
    assert "Customer" in scd2_df.columns
    assert "name" in scd2_df.columns

    # Row count should be preserved
    assert scd2_df.count() == sample_customer_data.count()


def test_scd2_timestamps_are_set(processor, sample_customer_data):
    """
    Test that SCD Type 2 timestamps are properly set
    """
    scd2_df = processor.add_scd2_columns(sample_customer_data)

    # Verify StartDate and OpTs are not NULL
    rows = scd2_df.collect()
    for row in rows:
        assert row["StartDate"] is not None
        assert row["OpTs"] is not None
        assert row["IsActive"] == True


# Integration Tests
def test_full_pipeline_with_clean_data(processor, sample_order_data, sample_customer_data):
    """
    Test complete pipeline flow with clean data
    """
    # Clean data
    customer_clean = processor.clean_data(sample_customer_data)
    order_clean = processor.clean_data(sample_order_data)

    # Aggregate
    aggregated = processor.aggregate_customer_spending(order_clean, customer_clean)

    # Add SCD2
    final_df = processor.add_scd2_columns(aggregated)

    # Verify final output
    assert final_df.count() == 3
    assert "IsActive" in final_df.columns
    assert "StartDate" in final_df.columns
    assert "EndDate" in final_df.columns
    assert "OpTs" in final_df.columns
    assert "TotalAmount" in final_df.columns


def test_full_pipeline_with_dirty_data(processor, dirty_order_data, dirty_customer_data):
    """
    Test complete pipeline flow with dirty data requiring cleaning
    """
    # Clean data
    customer_clean = processor.clean_data(dirty_customer_data)
    order_clean = processor.clean_data(dirty_order_data)

    # Should have cleaned records
    assert customer_clean.count() > 0
    assert order_clean.count() > 0

    # Aggregate (may result in 0 if no matching customers after cleaning)
    aggregated = processor.aggregate_customer_spending(order_clean, customer_clean)

    # Add SCD2
    final_df = processor.add_scd2_columns(aggregated)

    # Verify SCD2 columns exist
    assert "IsActive" in final_df.columns
    assert "OpTs" in final_df.columns


def test_column_names_match_trd(processor, sample_customer_data, sample_order_data):
    """
    Test that all column names match TRD specifications
    """
    # Customer columns
    customer_cols = set(sample_customer_data.columns)
    assert customer_cols == {"Customer", "name"}

    # Order columns
    order_cols = set(sample_order_data.columns)
    assert order_cols == {"Customer", "ordersummary", "TotalAmount"}

    # Aggregated columns
    aggregated = processor.aggregate_customer_spending(sample_order_data, sample_customer_data)
    agg_cols = set(aggregated.columns)
    assert agg_cols == {"Customer", "name", "TotalAmount"}

    # SCD2 columns
    scd2_df = processor.add_scd2_columns(aggregated)
    scd2_cols = set(scd2_df.columns)
    expected_scd2_cols = {"Customer", "name", "TotalAmount", "IsActive", "StartDate", "EndDate", "OpTs"}
    assert scd2_cols == expected_scd2_cols


def test_decimal_precision_preserved(processor, sample_order_data, sample_customer_data):
    """
    Test that Decimal type precision is preserved through pipeline
    """
    aggregated = processor.aggregate_customer_spending(sample_order_data, sample_customer_data)

    # Verify TotalAmount is still Decimal type
    total_amount_field = [f for f in aggregated.schema.fields if f.name == "TotalAmount"][0]
    assert isinstance(total_amount_field.dataType, DecimalType)


def test_empty_dataframe_handling(processor, spark):
    """
    Test pipeline handles empty DataFrames gracefully
    """
    empty_customer_schema = processor.get_customer_schema()
    empty_customer_df = spark.createDataFrame([], empty_customer_schema)

    # Clean empty DataFrame
    cleaned = processor.clean_data(empty_customer_df)
    assert cleaned.count() == 0

    # Add SCD2 to empty DataFrame
    scd2_df = processor.add_scd2_columns(cleaned)
    assert scd2_df.count() == 0
    assert "IsActive" in scd2_df.columns


def test_processor_initialization(spark):
    """
    Test CustomerOrderProcessor initialization
    """
    processor = CustomerOrderProcessor(spark_session=spark)

    assert processor.spark is not None
    assert processor.order_source_path == "s3://adif-sdlc/sdlc_wizard/orderdata/"
    assert processor.customer_source_path == "s3://adif-sdlc/catalog/sdlc_wizard/customer/"
    assert processor.output_path == "s3://adif-sdlc/analytics/customeraggregatespend/"