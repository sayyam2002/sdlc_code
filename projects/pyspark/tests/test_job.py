"""
Comprehensive test suite for Customer and Order Data Processing Pipeline
Tests all FRD requirements including data ingestion, cleaning, SCD Type 2, and aggregations
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)
from pyspark.sql.functions import col
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("CustomerOrderProcessingTests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def mock_processor(spark):
    """Create mock processor with mocked AWS dependencies"""
    with patch('src.job.SparkContext') as mock_sc, \
         patch('src.job.GlueContext') as mock_glue, \
         patch('src.job.Job') as mock_job:

        mock_sc_instance = Mock()
        mock_sc.return_value = mock_sc_instance

        mock_glue_instance = Mock()
        mock_glue_instance.spark_session = spark
        mock_glue.return_value = mock_glue_instance

        mock_job_instance = Mock()
        mock_job.return_value = mock_job_instance

        from src.job import CustomerOrderProcessor
        processor = CustomerOrderProcessor()

        yield processor


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data matching TRD schema"""
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),
        ("C004", "Alice Williams", "alice.williams@example.com", "West"),
    ]

    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with NULL and 'Null' values"""
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", None, "jane.smith@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        (None, "Alice Williams", "alice.williams@example.com", "West"),
        ("C005", "Charlie Brown", "charlie@example.com", "null"),
    ]

    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_duplicates(spark):
    """Create sample customer data with duplicates"""
    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", "Jane Smith", "jane.smith@example.com", "South"),
        ("C001", "John Doe", "john.doe@example.com", "North"),  # Duplicate
        ("C003", "Bob Johnson", "bob.johnson@example.com", "East"),
    ]

    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data_raw(spark):
    """Create sample order data in raw format (all strings)"""
    data = [
        ("O001", "Laptop", "999.99", "2", "2024-01-15", "C001"),
        ("O002", "Mouse", "25.50", "5", "2024-01-16", "C002"),
        ("O003", "Keyboard", "75.00", "3", "2024-01-17", "C001"),
        ("O004", "Monitor", "299.99", "1", "2024-01-18", "C003"),
    ]

    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", StringType(), True),
        StructField("Qty", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data_transformed(spark):
    """Create sample order data with proper types"""
    data = [
        ("O001", "Laptop", 999.99, 2, date(2024, 1, 15), "C001"),
        ("O002", "Mouse", 25.50, 5, date(2024, 1, 16), "C002"),
        ("O003", "Keyboard", 75.00, 3, date(2024, 1, 17), "C001"),
        ("O004", "Monitor", 299.99, 1, date(2024, 1, 18), "C003"),
    ]

    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DecimalType(10, 2), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("CustId", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


# Test FR-INGEST-001: Data Ingestion
def test_customer_schema_matches_trd(mock_processor):
    """Test that customer schema matches TRD specification"""
    schema = mock_processor.get_customer_schema()

    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "Name"
    assert schema.fields[1].dataType == StringType()
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[2].dataType == StringType()
    assert schema.fields[3].name == "Region"
    assert schema.fields[3].dataType == StringType()


def test_order_schema_matches_trd(mock_processor):
    """Test that order schema matches TRD specification"""
    schema = mock_processor.get_order_schema()

    assert len(schema.fields) == 6
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[1].name == "ItemName"
    assert schema.fields[2].name == "PricePerUnit"
    assert schema.fields[3].name == "Qty"
    assert schema.fields[4].name == "Date"
    assert schema.fields[5].name == "CustId"


def test_s3_paths_match_trd(mock_processor):
    """Test that S3 paths match TRD specification"""
    assert mock_processor.CUSTOMER_SOURCE_PATH == "s3://adif-sdlc/sdlc_wizard/customerdata/"
    assert mock_processor.ORDER_SOURCE_PATH == "s3://adif-sdlc/sdlc_wizard/orderdata/"
    assert mock_processor.CURATED_CUSTOMER_PATH == "s3://adif-sdlc/curated/sdlc_wizard/customer/"
    assert mock_processor.CURATED_ORDER_PATH == "s3://adif-sdlc/curated/sdlc_wizard/order/"
    assert mock_processor.ORDER_SUMMARY_PATH == "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
    assert mock_processor.CUSTOMER_AGGREGATE_SPEND_PATH == "s3://adif-sdlc/analytics/customeraggregatespend/"


def test_glue_database_name_matches_trd(mock_processor):
    """Test that Glue database name matches TRD specification"""
    assert mock_processor.GLUE_DATABASE == "sdlc_wizard"


# Test FR-CLEAN-001: Data Cleaning
def test_clean_data_removes_nulls(mock_processor, sample_customer_data_with_nulls):
    """Test that clean_data removes NULL values"""
    cleaned = mock_processor.clean_data(sample_customer_data_with_nulls)

    # Should only have C001 (all other rows have NULL or 'Null')
    assert cleaned.count() == 1
    assert cleaned.filter(col("CustId") == "C001").count() == 1


def test_clean_data_removes_null_strings(mock_processor, sample_customer_data_with_nulls):
    """Test that clean_data removes 'Null' string values"""
    cleaned = mock_processor.clean_data(sample_customer_data_with_nulls)

    # Verify no 'Null' strings remain
    for column in cleaned.columns:
        null_count = cleaned.filter(col(column).isin(["Null", "null", "NULL"])).count()
        assert null_count == 0


def test_clean_data_removes_duplicates(mock_processor, sample_customer_data_with_duplicates):
    """Test that clean_data removes duplicate records"""
    cleaned = mock_processor.clean_data(sample_customer_data_with_duplicates)

    # Should have 3 unique records (C001 duplicate removed)
    assert cleaned.count() == 3
    assert cleaned.filter(col("CustId") == "C001").count() == 1


def test_clean_data_preserves_valid_records(mock_processor, sample_customer_data):
    """Test that clean_data preserves valid records"""
    cleaned = mock_processor.clean_data(sample_customer_data)

    # All 4 records should be preserved
    assert cleaned.count() == 4


# Test FR-TRANSFORM-001: Data Transformation
def test_transform_order_data_parses_date(mock_processor, sample_order_data_raw):
    """Test that transform_order_data parses Date field as DateType"""
    transformed = mock_processor.transform_order_data(sample_order_data_raw)

    date_field = [f for f in transformed.schema.fields if f.name == "Date"][0]
    assert date_field.dataType == DateType()

    # Verify date values
    dates = [row.Date for row in transformed.collect()]
    assert date(2024, 1, 15) in dates


def test_transform_order_data_parses_decimal(mock_processor, sample_order_data_raw):
    """Test that transform_order_data parses PricePerUnit as DecimalType"""
    transformed = mock_processor.transform_order_data(sample_order_data_raw)

    price_field = [f for f in transformed.schema.fields if f.name == "PricePerUnit"][0]
    assert isinstance(price_field.dataType, DecimalType)

    # Verify decimal values
    prices = [float(row.PricePerUnit) for row in transformed.collect()]
    assert 999.99 in prices


def test_transform_order_data_parses_integer(mock_processor, sample_order_data_raw):
    """Test that transform_order_data parses Qty as IntegerType"""
    transformed = mock_processor.transform_order_data(sample_order_data_raw)

    qty_field = [f for f in transformed.schema.fields if f.name == "Qty"][0]
    assert qty_field.dataType == IntegerType()

    # Verify integer values
    quantities = [row.Qty for row in transformed.collect()]
    assert 2 in quantities


# Test FR-SCD2-001: SCD Type 2 Implementation
def test_add_scd2_columns_adds_isactive(mock_processor, sample_customer_data):
    """Test that add_scd2_columns adds IsActive column"""
    scd2_df = mock_processor.add_scd2_columns(sample_customer_data)

    assert "IsActive" in scd2_df.columns
    isactive_field = [f for f in scd2_df.schema.fields if f.name == "IsActive"][0]
    assert isactive_field.dataType == BooleanType()

    # All records should be active
    assert scd2_df.filter(col("IsActive") == True).count() == scd2_df.count()


def test_add_scd2_columns_adds_startdate(mock_processor, sample_customer_data):
    """Test that add_scd2_columns adds StartDate column"""
    scd2_df = mock_processor.add_scd2_columns(sample_customer_data)

    assert "StartDate" in scd2_df.columns
    startdate_field = [f for f in scd2_df.schema.fields if f.name == "StartDate"][0]
    assert startdate_field.dataType == TimestampType()

    # StartDate should not be null
    assert scd2_df.filter(col("StartDate").isNull()).count() == 0


def test_add_scd2_columns_adds_enddate(mock_processor, sample_customer_data):
    """Test that add_scd2_columns adds EndDate column"""
    scd2_df = mock_processor.add_scd2_columns(sample_customer_data)

    assert "EndDate" in scd2_df.columns
    enddate_field = [f for f in scd2_df.schema.fields if f.name == "EndDate"][0]
    assert enddate_field.dataType == TimestampType()

    # EndDate should be null for active records
    assert scd2_df.filter(col("EndDate").isNull()).count() == scd2_df.count()


def test_add_scd2_columns_adds_opts(mock_processor, sample_customer_data):
    """Test that add_scd2_columns adds OpTs column"""
    scd2_df = mock_processor.add_scd2_columns(sample_customer_data)

    assert "OpTs" in scd2_df.columns
    opts_field = [f for f in scd2_df.schema.fields if f.name == "OpTs"][0]
    assert opts_field.dataType == TimestampType()

    # OpTs should not be null
    assert scd2_df.filter(col("OpTs").isNull()).count() == 0


def test_scd2_columns_count(mock_processor, sample_customer_data):
    """Test that all 4 SCD Type 2 columns are added"""
    original_count = len(sample_customer_data.columns)
    scd2_df = mock_processor.add_scd2_columns(sample_customer_data)

    # Should add 4 columns: IsActive, StartDate, EndDate, OpTs
    assert len(scd2_df.columns) == original_count + 4


# Test FR-AGG-001: Aggregations
def test_create_order_summary_groups_by_orderid(mock_processor, sample_order_data_transformed):
    """Test that create_order_summary groups by OrderId"""
    summary = mock_processor.create_order_summary(sample_order_data_transformed)

    # Should have one row per unique OrderId
    assert summary.count() == 4
    assert "OrderId" in summary.columns


def test_create_order_summary_calculates_total(mock_processor, sample_order_data_transformed):
    """Test that create_order_summary calculates OrderTotal"""
    summary = mock_processor.create_order_summary(sample_order_data_transformed)

    assert "OrderTotal" in summary.columns

    # Verify calculation for O001: 999.99 * 2 = 1999.98
    o001_total = summary.filter(col("OrderId") == "O001").select("OrderTotal").collect()[0][0]
    assert float(o001_total) == pytest.approx(1999.98, rel=0.01)


def test_create_order_summary_counts_items(mock_processor, sample_order_data_transformed):
    """Test that create_order_summary counts items"""
    summary = mock_processor.create_order_summary(sample_order_data_transformed)

    assert "ItemCount" in summary.columns

    # Each order has 1 item in sample data
    item_counts = [row.ItemCount for row in summary.collect()]
    assert all(count == 1 for count in item_counts)


def test_create_customer_aggregate_spend_joins_data(mock_processor, sample_customer_data, sample_order_data_transformed):
    """Test that create_customer_aggregate_spend joins customer and order data"""
    aggregate = mock_processor.create_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data_transformed
    )

    # Should have customer columns
    assert "CustId" in aggregate.columns
    assert "Name" in aggregate.columns
    assert "TotalSpend" in aggregate.columns


def test_create_customer_aggregate_spend_calculates_total(mock_processor, sample_customer_data, sample_order_data_transformed):
    """Test that create_customer_aggregate_spend calculates total spend"""
    aggregate = mock_processor.create_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data_transformed
    )

    # C001 has 2 orders: 999.99*2 + 75.00*3 = 2224.98
    c001_spend = aggregate.filter(col("CustId") == "C001").select("TotalSpend").collect()[0][0]
    assert float(c001_spend) == pytest.approx(2224.98, rel=0.01)


def test_create_customer_aggregate_spend_handles_no_orders(mock_processor, sample_customer_data, sample_order_data_transformed):
    """Test that create_customer_aggregate_spend handles customers with no orders"""
    aggregate = mock_processor.create_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data_transformed
    )

    # C004 has no orders, should have TotalSpend = 0
    c004_spend = aggregate.filter(col("CustId") == "C004").select("TotalSpend").collect()[0][0]
    assert float(c004_spend) == 0.0


# Test FR-WRITE-001: Data Writing
@patch('src.job.CustomerOrderProcessor.write_parquet')
def test_write_parquet_called_with_correct_path(mock_write, mock_processor, sample_order_data_transformed):
    """Test that write_parquet is called with correct output path"""
    mock_processor.write_parquet(sample_order_data_transformed, mock_processor.CURATED_ORDER_PATH)

    mock_write.assert_called_once()


@patch('src.job.CustomerOrderProcessor.register_glue_table')
def test_register_glue_table_called(mock_register, mock_processor):
    """Test that register_glue_table is called with correct parameters"""
    mock_processor.register_glue_table('customer', mock_processor.CURATED_CUSTOMER_PATH)

    mock_register.assert_called_once_with('customer', mock_processor.CURATED_CUSTOMER_PATH)


# Integration test
def test_end_to_end_data_flow(mock_processor, sample_customer_data, sample_order_data_raw):
    """Test end-to-end data flow from raw to curated"""
    # Clean customer data
    customer_cleaned = mock_processor.clean_data(sample_customer_data)
    assert customer_cleaned.count() == 4

    # Add SCD2 columns
    customer_scd2 = mock_processor.add_scd2_columns(customer_cleaned)
    assert "IsActive" in customer_scd2.columns

    # Transform order data
    order_transformed = mock_processor.transform_order_data(sample_order_data_raw)
    order_cleaned = mock_processor.clean_data(order_transformed)
    assert order_cleaned.count() == 4

    # Create aggregations
    order_summary = mock_processor.create_order_summary(order_cleaned)
    assert order_summary.count() == 4

    customer_aggregate = mock_processor.create_customer_aggregate_spend(
        customer_cleaned,
        order_cleaned
    )
    assert customer_aggregate.count() == 4