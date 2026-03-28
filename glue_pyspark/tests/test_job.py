import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from pyspark.sql.functions import col
import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from job import (
    CUSTOMER_SCHEMA, ORDER_SCHEMA,
    clean_data, add_scd2_columns, aggregate_customer_spend,
    create_order_summary
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("TestSDLCWizardJob") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing."""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West")
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing."""
    data = [
        ("O001", "Laptop", 1000.0, 2, "2024-01-15", "C001"),
        ("O002", "Mouse", 25.0, 5, "2024-01-16", "C001"),
        ("O003", "Keyboard", 75.0, 3, "2024-01-17", "C002"),
        ("O004", "Monitor", 300.0, 1, "2024-01-18", "C003")
    ]
    return spark.createDataFrame(data, ORDER_SCHEMA)


@pytest.fixture
def customer_data_with_nulls(spark):
    """Create customer data with NULL values for testing."""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", None, "East"),
        (None, "Alice Brown", "alice@example.com", "West")
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


@pytest.fixture
def customer_data_with_null_strings(spark):
    """Create customer data with 'Null' string values for testing."""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Null", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "null", "East"),
        ("C004", "Alice Brown", "alice@example.com", "NULL")
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


@pytest.fixture
def customer_data_with_duplicates(spark):
    """Create customer data with duplicate records for testing."""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C002", "Jane Smith", "jane@example.com", "South")
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


# Test FR-INGEST-001: Verify customer data schema
def test_customer_schema_validation(sample_customer_data):
    """Test that customer data matches expected schema from TRD."""
    assert sample_customer_data.schema == CUSTOMER_SCHEMA
    assert "CustId" in sample_customer_data.columns
    assert "Name" in sample_customer_data.columns
    assert "EmailId" in sample_customer_data.columns
    assert "Region" in sample_customer_data.columns


# Test FR-INGEST-002: Verify order data schema
def test_order_schema_validation(sample_order_data):
    """Test that order data matches expected schema from TRD."""
    assert sample_order_data.schema == ORDER_SCHEMA
    assert "OrderId" in sample_order_data.columns
    assert "ItemName" in sample_order_data.columns
    assert "PricePerUnit" in sample_order_data.columns
    assert "Qty" in sample_order_data.columns
    assert "Date" in sample_order_data.columns
    assert "CustId" in sample_order_data.columns


# Test FR-CLEAN-001: Remove NULL values
def test_clean_data_removes_nulls(customer_data_with_nulls):
    """Test that clean_data removes rows with NULL values."""
    cleaned = clean_data(customer_data_with_nulls)
    assert cleaned.count() == 1
    assert cleaned.filter(col("CustId").isNull()).count() == 0
    assert cleaned.filter(col("Name").isNull()).count() == 0


# Test FR-CLEAN-002: Remove 'Null' string values
def test_clean_data_removes_null_strings(customer_data_with_null_strings):
    """Test that clean_data removes rows with 'Null' string values."""
    cleaned = clean_data(customer_data_with_null_strings)
    assert cleaned.count() == 1
    result = cleaned.collect()
    assert result[0]["CustId"] == "C001"


# Test FR-CLEAN-003: Remove duplicate records
def test_clean_data_removes_duplicates(customer_data_with_duplicates):
    """Test that clean_data removes duplicate records."""
    cleaned = clean_data(customer_data_with_duplicates)
    assert cleaned.count() == 2
    assert cleaned.filter(col("CustId") == "C001").count() == 1
    assert cleaned.filter(col("CustId") == "C002").count() == 1


# Test FR-SCD2-001: Add IsActive column
def test_scd2_adds_isactive_column(sample_customer_data):
    """Test that SCD Type 2 adds IsActive column."""
    scd2_df = add_scd2_columns(sample_customer_data)
    assert "IsActive" in scd2_df.columns
    assert scd2_df.schema["IsActive"].dataType == BooleanType()
    assert scd2_df.filter(col("IsActive") == True).count() == 4


# Test FR-SCD2-002: Add StartDate column
def test_scd2_adds_startdate_column(sample_customer_data):
    """Test that SCD Type 2 adds StartDate column."""
    scd2_df = add_scd2_columns(sample_customer_data)
    assert "StartDate" in scd2_df.columns
    assert scd2_df.schema["StartDate"].dataType == TimestampType()
    assert scd2_df.filter(col("StartDate").isNotNull()).count() == 4


# Test FR-SCD2-003: Add EndDate column
def test_scd2_adds_enddate_column(sample_customer_data):
    """Test that SCD Type 2 adds EndDate column."""
    scd2_df = add_scd2_columns(sample_customer_data)
    assert "EndDate" in scd2_df.columns
    assert scd2_df.schema["EndDate"].dataType == TimestampType()
    # For active records, EndDate should be NULL
    assert scd2_df.filter(col("IsActive") == True).filter(col("EndDate").isNull()).count() == 4


# Test FR-SCD2-004: Add OpTs column
def test_scd2_adds_opts_column(sample_customer_data):
    """Test that SCD Type 2 adds OpTs column."""
    scd2_df = add_scd2_columns(sample_customer_data)
    assert "OpTs" in scd2_df.columns
    assert scd2_df.schema["OpTs"].dataType == TimestampType()
    assert scd2_df.filter(col("OpTs").isNotNull()).count() == 4


# Test FR-AGG-001: Aggregate customer spend
def test_aggregate_customer_spend_calculation(sample_customer_data, sample_order_data):
    """Test that customer spend aggregation calculates correctly."""
    result = aggregate_customer_spend(sample_customer_data, sample_order_data)

    # C001: (1000*2) + (25*5) = 2125
    c001_spend = result.filter(col("CustId") == "C001").select("TotalSpend").collect()[0][0]
    assert c001_spend == 2125.0

    # C002: 75*3 = 225
    c002_spend = result.filter(col("CustId") == "C002").select("TotalSpend").collect()[0][0]
    assert c002_spend == 225.0

    # C003: 300*1 = 300
    c003_spend = result.filter(col("CustId") == "C003").select("TotalSpend").collect()[0][0]
    assert c003_spend == 300.0


# Test FR-AGG-002: Count orders per customer
def test_aggregate_customer_order_count(sample_customer_data, sample_order_data):
    """Test that order count aggregation calculates correctly."""
    result = aggregate_customer_spend(sample_customer_data, sample_order_data)

    c001_count = result.filter(col("CustId") == "C001").select("OrderCount").collect()[0][0]
    assert c001_count == 2

    c002_count = result.filter(col("CustId") == "C002").select("OrderCount").collect()[0][0]
    assert c002_count == 1


# Test FR-AGG-003: Handle customers with no orders
def test_aggregate_handles_customers_without_orders(sample_customer_data, sample_order_data):
    """Test that customers without orders have zero spend."""
    result = aggregate_customer_spend(sample_customer_data, sample_order_data)

    # C004 has no orders
    c004_spend = result.filter(col("CustId") == "C004").select("TotalSpend").collect()[0][0]
    assert c004_spend == 0.0

    c004_count = result.filter(col("CustId") == "C004").select("OrderCount").collect()[0][0]
    assert c004_count == 0


# Test FR-JOIN-001: Create order summary with customer details
def test_create_order_summary_joins_correctly(sample_customer_data, sample_order_data):
    """Test that order summary joins customer and order data correctly."""
    result = create_order_summary(sample_customer_data, sample_order_data)

    assert result.count() == 4
    assert "OrderId" in result.columns
    assert "CustomerName" in result.columns
    assert "EmailId" in result.columns
    assert "Region" in result.columns
    assert "TotalAmount" in result.columns


# Test FR-JOIN-002: Calculate total amount per order
def test_order_summary_calculates_total_amount(sample_customer_data, sample_order_data):
    """Test that order summary calculates TotalAmount correctly."""
    result = create_order_summary(sample_customer_data, sample_order_data)

    # O001: 1000 * 2 = 2000
    o001_total = result.filter(col("OrderId") == "O001").select("TotalAmount").collect()[0][0]
    assert o001_total == 2000.0

    # O002: 25 * 5 = 125
    o002_total = result.filter(col("OrderId") == "O002").select("TotalAmount").collect()[0][0]
    assert o002_total == 125.0


# Test FR-SCHEMA-001: Verify all customer columns present
def test_customer_schema_all_columns_present(sample_customer_data):
    """Test that all customer columns from TRD are present."""
    expected_columns = ["CustId", "Name", "EmailId", "Region"]
    for col_name in expected_columns:
        assert col_name in sample_customer_data.columns


# Test FR-SCHEMA-002: Verify all order columns present
def test_order_schema_all_columns_present(sample_order_data):
    """Test that all order columns from TRD are present."""
    expected_columns = ["OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId"]
    for col_name in expected_columns:
        assert col_name in sample_order_data.columns


# Test FR-TRANSFORM-001: Verify data types after cleaning
def test_cleaned_data_maintains_types(sample_customer_data):
    """Test that cleaned data maintains correct data types."""
    cleaned = clean_data(sample_customer_data)
    assert cleaned.schema["CustId"].dataType == StringType()
    assert cleaned.schema["Name"].dataType == StringType()
    assert cleaned.schema["EmailId"].dataType == StringType()
    assert cleaned.schema["Region"].dataType == StringType()


# Test FR-OUTPUT-001: Verify SCD2 output has all required columns
def test_scd2_output_has_all_columns(sample_customer_data):
    """Test that SCD Type 2 output contains all required columns."""
    scd2_df = add_scd2_columns(sample_customer_data)

    # Original columns
    assert "CustId" in scd2_df.columns
    assert "Name" in scd2_df.columns
    assert "EmailId" in scd2_df.columns
    assert "Region" in scd2_df.columns

    # SCD Type 2 columns
    assert "IsActive" in scd2_df.columns
    assert "StartDate" in scd2_df.columns
    assert "EndDate" in scd2_df.columns
    assert "OpTs" in scd2_df.columns