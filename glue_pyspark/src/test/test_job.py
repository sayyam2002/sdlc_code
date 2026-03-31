"""
Unit Tests for AWS Glue PySpark Job - Customer Order Data Processing

Tests cover:
- FR-INGEST-001: Data ingestion from S3
- FR-CLEAN-001: Data cleaning (NULL, 'Null', duplicates)
- FR-SCD2-001: SCD Type 2 implementation
- FR-AGGREGATE-001: Customer aggregate spend calculation
- FR-SUMMARY-001: Order summary generation
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType
)
from pyspark.sql.functions import col
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../main')))

from job import (
    get_customer_schema,
    get_order_schema,
    clean_data,
    add_scd2_columns,
    apply_scd2_logic,
    generate_order_summary,
    calculate_customer_aggregate_spend
)


@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing.
    """
    spark = SparkSession.builder \
        .appName("TestCustomerOrderProcessing") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """
    Create sample customer data for testing.
    """
    schema = get_customer_schema()
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West")
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """
    Create sample order data for testing.
    """
    schema = get_order_schema()
    data = [
        ("O001", "C001", "2024-01-15", 100.50),
        ("O002", "C001", "2024-01-20", 250.75),
        ("O003", "C002", "2024-01-18", 175.00),
        ("O004", "C003", "2024-01-22", 300.25),
        ("O005", "C002", "2024-01-25", 125.50)
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_customer_data(spark):
    """
    Create customer data with NULL and 'Null' values for cleaning tests.
    """
    schema = get_customer_schema()
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C001", "John Doe", "john@example.com", "North")
    ]
    return spark.createDataFrame(data, schema)


def test_fr_ingest_001_customer_schema_validation():
    """
    FR-INGEST-001: Verify customer schema matches TRD requirements.

    Expected Schema:
    - CustId (string)
    - Name (string)
    - EmailId (string)
    - Region (string)
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


def test_fr_ingest_002_order_schema_validation():
    """
    FR-INGEST-002: Verify order schema matches TRD requirements.

    Expected Schema:
    - OrderId (string)
    - CustId (string)
    - OrderDate (string)
    - Amount (double)
    """
    schema = get_order_schema()

    assert len(schema.fields) == 4
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "CustId"
    assert schema.fields[1].dataType == StringType()
    assert schema.fields[2].name == "OrderDate"
    assert schema.fields[2].dataType == StringType()
    assert schema.fields[3].name == "Amount"
    assert schema.fields[3].dataType == DoubleType()


def test_fr_clean_001_remove_null_values(spark, dirty_customer_data):
    """
    FR-CLEAN-001: Verify NULL values are removed from data.

    Test Data: 5 records (1 with NULL Name)
    Expected: 4 records after cleaning
    """
    cleaned_df = clean_data(dirty_customer_data)

    assert cleaned_df.count() == 3

    null_count = cleaned_df.filter(col("Name").isNull()).count()
    assert null_count == 0


def test_fr_clean_002_remove_null_string_values(spark, dirty_customer_data):
    """
    FR-CLEAN-002: Verify 'Null' string values are removed from data.

    Test Data: Record with EmailId='Null'
    Expected: Record removed after cleaning
    """
    cleaned_df = clean_data(dirty_customer_data)

    null_string_count = cleaned_df.filter(col("EmailId") == "Null").count()
    assert null_string_count == 0


def test_fr_clean_003_remove_duplicate_records(spark, dirty_customer_data):
    """
    FR-CLEAN-003: Verify duplicate records are removed.

    Test Data: 2 identical records for C001
    Expected: Only 1 record for C001 after cleaning
    """
    cleaned_df = clean_data(dirty_customer_data)

    c001_count = cleaned_df.filter(col("CustId") == "C001").count()
    assert c001_count == 1


def test_fr_clean_004_all_cleaning_rules_applied(spark, dirty_customer_data):
    """
    FR-CLEAN-004: Verify all cleaning rules applied together.

    Test Data: 5 records (1 NULL, 1 'Null', 1 duplicate)
    Expected: 3 clean records
    """
    cleaned_df = clean_data(dirty_customer_data)

    assert cleaned_df.count() == 3

    for row in cleaned_df.collect():
        for field in row:
            assert field is not None
            assert field != "Null"
            assert field != "null"
            assert field != "NULL"


def test_fr_scd2_001_add_scd2_columns(spark, sample_customer_data):
    """
    FR-SCD2-001: Verify SCD Type 2 columns are added correctly.

    Expected Columns:
    - IsActive (boolean, default True)
    - StartDate (timestamp, current)
    - EndDate (timestamp, NULL)
    - OpTs (timestamp, current)
    """
    df_with_scd2 = add_scd2_columns(sample_customer_data, "CustId")

    assert "IsActive" in df_with_scd2.columns
    assert "StartDate" in df_with_scd2.columns
    assert "EndDate" in df_with_scd2.columns
    assert "OpTs" in df_with_scd2.columns

    first_row = df_with_scd2.first()
    assert first_row["IsActive"] == True
    assert first_row["StartDate"] is not None
    assert first_row["EndDate"] is None
    assert first_row["OpTs"] is not None


def test_fr_scd2_002_initial_load_all_active(spark, sample_customer_data):
    """
    FR-SCD2-002: Verify initial load marks all records as active.

    Test: First load with no existing data
    Expected: All records have IsActive=True
    """
    df_with_scd2 = add_scd2_columns(sample_customer_data, "CustId")
    result_df = apply_scd2_logic(df_with_scd2, None, "CustId")

    assert result_df.count() == 4
    active_count = result_df.filter(col("IsActive") == True).count()
    assert active_count == 4


def test_fr_scd2_003_upsert_new_records(spark, sample_customer_data):
    """
    FR-SCD2-003: Verify new records are inserted as active.

    Test: Add new customer to existing data
    Expected: New record added with IsActive=True
    """
    existing_df = add_scd2_columns(sample_customer_data, "CustId")

    new_data = spark.createDataFrame([
        ("C005", "Charlie Wilson", "charlie@example.com", "Central")
    ], get_customer_schema())
    new_df = add_scd2_columns(new_data, "CustId")

    result_df = apply_scd2_logic(new_df, existing_df, "CustId")

    c005_records = result_df.filter(col("CustId") == "C005")
    assert c005_records.count() >= 1

    active_c005 = c005_records.filter(col("IsActive") == True)
    assert active_c005.count() == 1


def test_fr_scd2_004_update_existing_records(spark, sample_customer_data):
    """
    FR-SCD2-004: Verify updated records create new version and expire old.

    Test: Update customer email
    Expected: Old record IsActive=False with EndDate, new record IsActive=True
    """
    existing_df = add_scd2_columns(sample_customer_data, "CustId")

    updated_data = spark.createDataFrame([
        ("C001", "John Doe", "john.new@example.com", "North")
    ], get_customer_schema())
    updated_df = add_scd2_columns(updated_data, "CustId")

    result_df = apply_scd2_logic(updated_df, existing_df, "CustId")

    c001_records = result_df.filter(col("CustId") == "C001")
    assert c001_records.count() >= 2

    active_c001 = c001_records.filter(col("IsActive") == True)
    assert active_c001.count() == 1
    assert active_c001.first()["EmailId"] == "john.new@example.com"

    inactive_c001 = c001_records.filter(col("IsActive") == False)
    assert inactive_c001.count() >= 1
    for row in inactive_c001.collect():
        assert row["EndDate"] is not None


def test_fr_scd2_005_unchanged_records_remain_active(spark, sample_customer_data):
    """
    FR-SCD2-005: Verify unchanged records remain active.

    Test: Reload same data
    Expected: Records remain IsActive=True with original timestamps
    """
    existing_df = add_scd2_columns(sample_customer_data, "CustId")

    same_data = sample_customer_data
    same_df = add_scd2_columns(same_data, "CustId")

    result_df = apply_scd2_logic(same_df, existing_df, "CustId")

    c002_records = result_df.filter(col("CustId") == "C002")
    active_c002 = c002_records.filter(col("IsActive") == True)
    assert active_c002.count() == 1


def test_fr_aggregate_001_calculate_total_spend(spark, sample_order_data):
    """
    FR-AGGREGATE-001: Verify customer total spend calculation.

    Test Data:
    - C001: 2 orders (100.50 + 250.75 = 351.25)
    - C002: 2 orders (175.00 + 125.50 = 300.50)
    - C003: 1 order (300.25)
    """
    aggregate_df = calculate_customer_aggregate_spend(sample_order_data)

    assert aggregate_df.count() == 3

    c001_spend = aggregate_df.filter(col("CustId") == "C001").first()["TotalSpend"]
    assert abs(c001_spend - 351.25) < 0.01

    c002_spend = aggregate_df.filter(col("CustId") == "C002").first()["TotalSpend"]
    assert abs(c002_spend - 300.50) < 0.01

    c003_spend = aggregate_df.filter(col("CustId") == "C003").first()["TotalSpend"]
    assert abs(c003_spend - 300.25) < 0.01


def test_fr_aggregate_002_calculate_order_count(spark, sample_order_data):
    """
    FR-AGGREGATE-002: Verify customer order count calculation.

    Test Data:
    - C001: 2 orders
    - C002: 2 orders
    - C003: 1 order
    """
    aggregate_df = calculate_customer_aggregate_spend(sample_order_data)

    c001_count = aggregate_df.filter(col("CustId") == "C001").first()["OrderCount"]
    assert c001_count == 2

    c002_count = aggregate_df.filter(col("CustId") == "C002").first()["OrderCount"]
    assert c002_count == 2

    c003_count = aggregate_df.filter(col("CustId") == "C003").first()["OrderCount"]
    assert c003_count == 1


def test_fr_aggregate_003_all_customers_included(spark, sample_order_data):
    """
    FR-AGGREGATE-003: Verify all customers with orders are included.

    Test Data: 3 unique customers
    Expected: 3 records in aggregate
    """
    aggregate_df = calculate_customer_aggregate_spend(sample_order_data)

    assert aggregate_df.count() == 3

    customer_ids = [row["CustId"] for row in aggregate_df.collect()]
    assert "C001" in customer_ids
    assert "C002" in customer_ids
    assert "C003" in customer_ids


def test_fr_summary_001_join_customer_order_data(spark, sample_customer_data, sample_order_data):
    """
    FR-SUMMARY-001: Verify order summary joins customer and order data.

    Expected Columns: OrderId, CustId, Name, EmailId, Region, OrderDate, Amount
    """
    customer_with_scd2 = add_scd2_columns(sample_customer_data, "CustId")
    order_summary_df = generate_order_summary(spark, customer_with_scd2, sample_order_data)

    expected_columns = ["OrderId", "CustId", "Name", "EmailId", "Region", "OrderDate", "Amount"]
    for col_name in expected_columns:
        assert col_name in order_summary_df.columns


def test_fr_summary_002_all_orders_included(spark, sample_customer_data, sample_order_data):
    """
    FR-SUMMARY-002: Verify all orders are included in summary.

    Test Data: 5 orders
    Expected: 5 records in summary
    """
    customer_with_scd2 = add_scd2_columns(sample_customer_data, "CustId")
    order_summary_df = generate_order_summary(spark, customer_with_scd2, sample_order_data)

    assert order_summary_df.count() == 5


def test_fr_summary_003_customer_details_populated(spark, sample_customer_data, sample_order_data):
    """
    FR-SUMMARY-003: Verify customer details are correctly populated.

    Test: Check order O001 has correct customer details
    Expected: Name='John Doe', EmailId='john@example.com', Region='North'
    """
    customer_with_scd2 = add_scd2_columns(sample_customer_data, "CustId")
    order_summary_df = generate_order_summary(spark, customer_with_scd2, sample_order_data)

    o001_record = order_summary_df.filter(col("OrderId") == "O001").first()
    assert o001_record["Name"] == "John Doe"
    assert o001_record["EmailId"] == "john@example.com"
    assert o001_record["Region"] == "North"


def test_fr_summary_004_only_active_customers(spark, sample_customer_data, sample_order_data):
    """
    FR-SUMMARY-004: Verify only active customer records are used in summary.

    Test: Create inactive customer record
    Expected: Order summary uses only active customer data
    """
    customer_with_scd2 = add_scd2_columns(sample_customer_data, "CustId")

    inactive_customer = spark.createDataFrame([
        ("C001", "Old Name", "old@example.com", "South", False,
         datetime.now(), datetime.now(), datetime.now())
    ], customer_with_scd2.schema)

    combined_customers = customer_with_scd2.union(inactive_customer)

    order_summary_df = generate_order_summary(spark, combined_customers, sample_order_data)

    o001_record = order_summary_df.filter(col("OrderId") == "O001").first()
    assert o001_record["Name"] == "John Doe"
    assert o001_record["Name"] != "Old Name"


def test_integration_001_end_to_end_workflow(spark, sample_customer_data, sample_order_data):
    """
    INTEGRATION-001: Verify complete end-to-end workflow.

    Steps:
    1. Clean customer and order data
    2. Apply SCD Type 2 to customers
    3. Generate order summary
    4. Calculate aggregate spend
    """
    cleaned_customer_df = clean_data(sample_customer_data)
    cleaned_order_df = clean_data(sample_order_data)

    customer_with_scd2 = add_scd2_columns(cleaned_customer_df, "CustId")
    final_customer_df = apply_scd2_logic(customer_with_scd2, None, "CustId")

    order_summary_df = generate_order_summary(spark, final_customer_df, cleaned_order_df)
    aggregate_spend_df = calculate_customer_aggregate_spend(cleaned_order_df)

    assert final_customer_df.count() == 4
    assert order_summary_df.count() == 5
    assert aggregate_spend_df.count() == 3

    assert all(col_name in order_summary_df.columns
               for col_name in ["OrderId", "CustId", "Name", "EmailId", "Region", "OrderDate", "Amount"])
    assert all(col_name in aggregate_spend_df.columns
               for col_name in ["CustId", "TotalSpend", "OrderCount"])


def test_data_quality_001_no_null_in_cleaned_data(spark, dirty_customer_data):
    """
    DATA-QUALITY-001: Verify no NULL values in cleaned data.
    """
    cleaned_df = clean_data(dirty_customer_data)

    for column in cleaned_df.columns:
        null_count = cleaned_df.filter(col(column).isNull()).count()
        assert null_count == 0, f"Column {column} contains NULL values"


def test_data_quality_002_no_null_strings_in_cleaned_data(spark, dirty_customer_data):
    """
    DATA-QUALITY-002: Verify no 'Null' strings in cleaned data.
    """
    cleaned_df = clean_data(dirty_customer_data)

    for column in cleaned_df.columns:
        null_string_count = cleaned_df.filter(
            (col(column) == "Null") |
            (col(column) == "null") |
            (col(column) == "NULL")
        ).count()
        assert null_string_count == 0, f"Column {column} contains 'Null' strings"


def test_data_quality_003_unique_active_records_per_key(spark, sample_customer_data):
    """
    DATA-QUALITY-003: Verify only one active record per customer.
    """
    customer_with_scd2 = add_scd2_columns(sample_customer_data, "CustId")
    final_df = apply_scd2_logic(customer_with_scd2, None, "CustId")

    active_df = final_df.filter(col("IsActive") == True)

    customer_counts = active_df.groupBy("CustId").count()
    max_count = customer_counts.agg({"count": "max"}).first()[0]

    assert max_count == 1, "Multiple active records found for same customer"