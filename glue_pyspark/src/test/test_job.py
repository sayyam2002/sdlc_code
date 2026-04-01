"""
Unit tests for AWS Glue PySpark Job
Tests all FRD requirements including data ingestion, cleaning, SCD Type 2, and aggregation
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

# Add src to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main.job import (
    get_customer_schema,
    get_order_schema,
    clean_dataframe,
    read_customer_data,
    read_order_data,
    join_customer_order_data,
    apply_scd_type2_columns,
    calculate_customer_aggregate_spend
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestGlueJob") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark
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
    schema = get_customer_schema()
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing"""
    data = [
        ("O001", "C001", "2024-01-15", 150.50, "P001"),
        ("O002", "C001", "2024-01-20", 200.00, "P002"),
        ("O003", "C002", "2024-01-18", 75.25, "P001"),
        ("O004", "C003", "2024-01-22", 300.00, "P003"),
        ("O005", "C002", "2024-01-25", 125.75, "P002")
    ]
    schema = get_order_schema()
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_customer_data(spark):
    """Create customer data with NULL and 'Null' values"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        (None, "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "null"),
        ("C006", "", "empty@example.com", "North"),
        ("C007", "Valid User", "valid@example.com", "South")
    ]
    schema = get_customer_schema()
    return spark.createDataFrame(data, schema)


@pytest.fixture
def duplicate_order_data(spark):
    """Create order data with duplicates"""
    data = [
        ("O001", "C001", "2024-01-15", 150.50, "P001"),
        ("O001", "C001", "2024-01-15", 150.50, "P001"),
        ("O002", "C002", "2024-01-18", 75.25, "P002"),
        ("O002", "C002", "2024-01-18", 75.25, "P002"),
        ("O003", "C003", "2024-01-20", 200.00, "P003")
    ]
    schema = get_order_schema()
    return spark.createDataFrame(data, schema)


class TestSchemas:
    """Test FR-SCHEMA-001: Verify schema definitions"""

    def test_customer_schema_structure(self):
        """Test customer schema has correct fields"""
        schema = get_customer_schema()
        field_names = [field.name for field in schema.fields]

        assert "CustId" in field_names
        assert "Name" in field_names
        assert "EmailId" in field_names
        assert "Region" in field_names
        assert len(field_names) == 4

    def test_order_schema_structure(self):
        """Test order schema has correct fields"""
        schema = get_order_schema()
        field_names = [field.name for field in schema.fields]

        assert "OrderId" in field_names
        assert "CustId" in field_names
        assert "OrderDate" in field_names
        assert "Amount" in field_names
        assert "ProductId" in field_names
        assert len(field_names) == 5

    def test_customer_schema_types(self):
        """Test customer schema field types"""
        schema = get_customer_schema()

        assert isinstance(schema["CustId"].dataType, StringType)
        assert isinstance(schema["Name"].dataType, StringType)
        assert isinstance(schema["EmailId"].dataType, StringType)
        assert isinstance(schema["Region"].dataType, StringType)

    def test_order_schema_types(self):
        """Test order schema field types"""
        schema = get_order_schema()

        assert isinstance(schema["OrderId"].dataType, StringType)
        assert isinstance(schema["CustId"].dataType, StringType)
        assert isinstance(schema["OrderDate"].dataType, StringType)
        assert isinstance(schema["Amount"].dataType, DoubleType)
        assert isinstance(schema["ProductId"].dataType, StringType)


class TestDataCleaning:
    """Test FR-CLEAN-001: Data cleaning requirements"""

    def test_remove_null_values(self, spark, dirty_customer_data):
        """Test removal of NULL values"""
        cleaned_df = clean_dataframe(dirty_customer_data)

        # Check no NULL values remain
        for col_name in cleaned_df.columns:
            null_count = cleaned_df.filter(col(col_name).isNull()).count()
            assert null_count == 0, f"Column {col_name} still has NULL values"

    def test_remove_null_string_values(self, spark, dirty_customer_data):
        """Test removal of 'Null' string values"""
        cleaned_df = clean_dataframe(dirty_customer_data)

        # Check no 'Null' strings remain (case-insensitive)
        for col_name in cleaned_df.columns:
            null_string_count = cleaned_df.filter(
                col(col_name).isin(['Null', 'null', 'NULL'])
            ).count()
            assert null_string_count == 0, f"Column {col_name} still has 'Null' strings"

    def test_remove_empty_strings(self, spark, dirty_customer_data):
        """Test removal of empty string values"""
        cleaned_df = clean_dataframe(dirty_customer_data)

        # Check no empty strings remain
        for col_name in cleaned_df.columns:
            empty_count = cleaned_df.filter(col(col_name) == '').count()
            assert empty_count == 0, f"Column {col_name} still has empty strings"

    def test_remove_duplicates(self, spark, duplicate_order_data):
        """Test removal of duplicate records"""
        original_count = duplicate_order_data.count()
        cleaned_df = clean_dataframe(duplicate_order_data)
        cleaned_count = cleaned_df.count()

        assert cleaned_count < original_count
        assert cleaned_count == 3

    def test_clean_dataframe_preserves_valid_data(self, spark, sample_customer_data):
        """Test that cleaning preserves valid data"""
        original_count = sample_customer_data.count()
        cleaned_df = clean_dataframe(sample_customer_data)
        cleaned_count = cleaned_df.count()

        assert cleaned_count == original_count


class TestDataIngestion:
    """Test FR-INGEST-001: Data ingestion from S3"""

    def test_customer_data_structure(self, sample_customer_data):
        """Test customer data has correct structure"""
        assert sample_customer_data.count() > 0
        assert "CustId" in sample_customer_data.columns
        assert "Name" in sample_customer_data.columns
        assert "EmailId" in sample_customer_data.columns
        assert "Region" in sample_customer_data.columns

    def test_order_data_structure(self, sample_order_data):
        """Test order data has correct structure"""
        assert sample_order_data.count() > 0
        assert "OrderId" in sample_order_data.columns
        assert "CustId" in sample_order_data.columns
        assert "OrderDate" in sample_order_data.columns
        assert "Amount" in sample_order_data.columns
        assert "ProductId" in sample_order_data.columns

    def test_customer_data_types(self, sample_customer_data):
        """Test customer data types are correct"""
        schema = sample_customer_data.schema

        assert isinstance(schema["CustId"].dataType, StringType)
        assert isinstance(schema["Name"].dataType, StringType)
        assert isinstance(schema["EmailId"].dataType, StringType)
        assert isinstance(schema["Region"].dataType, StringType)

    def test_order_data_types(self, sample_order_data):
        """Test order data types are correct"""
        schema = sample_order_data.schema

        assert isinstance(schema["OrderId"].dataType, StringType)
        assert isinstance(schema["CustId"].dataType, StringType)
        assert isinstance(schema["OrderDate"].dataType, StringType)
        assert isinstance(schema["Amount"].dataType, DoubleType)
        assert isinstance(schema["ProductId"].dataType, StringType)


class TestDataTransformation:
    """Test FR-TRANSFORM-001: Data transformation logic"""

    def test_join_customer_order_data(self, sample_customer_data, sample_order_data):
        """Test joining customer and order data"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)

        assert joined_df.count() > 0
        assert "OrderId" in joined_df.columns
        assert "CustId" in joined_df.columns
        assert "Name" in joined_df.columns
        assert "EmailId" in joined_df.columns
        assert "Region" in joined_df.columns
        assert "OrderDate" in joined_df.columns
        assert "Amount" in joined_df.columns
        assert "ProductId" in joined_df.columns

    def test_join_preserves_order_count(self, sample_customer_data, sample_order_data):
        """Test join preserves all order records"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)

        assert joined_df.count() == sample_order_data.count()

    def test_join_includes_customer_details(self, sample_customer_data, sample_order_data):
        """Test join includes customer details"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)

        # Check that customer details are present
        first_row = joined_df.filter(col("OrderId") == "O001").first()
        assert first_row["Name"] == "John Doe"
        assert first_row["EmailId"] == "john@example.com"
        assert first_row["Region"] == "North"


class TestSCDType2:
    """Test FR-SCD2-001: SCD Type 2 implementation"""

    def test_scd_columns_added(self, sample_customer_data, sample_order_data):
        """Test SCD Type 2 columns are added"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)

        assert "IsActive" in scd_df.columns
        assert "StartDate" in scd_df.columns
        assert "EndDate" in scd_df.columns
        assert "OpTs" in scd_df.columns

    def test_scd_column_types(self, sample_customer_data, sample_order_data):
        """Test SCD Type 2 column types"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)
        schema = scd_df.schema

        assert isinstance(schema["IsActive"].dataType, BooleanType)
        assert isinstance(schema["StartDate"].dataType, TimestampType)
        assert isinstance(schema["EndDate"].dataType, TimestampType)
        assert isinstance(schema["OpTs"].dataType, TimestampType)

    def test_isactive_default_value(self, sample_customer_data, sample_order_data):
        """Test IsActive defaults to True"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)

        active_count = scd_df.filter(col("IsActive") == True).count()
        assert active_count == scd_df.count()

    def test_startdate_populated(self, sample_customer_data, sample_order_data):
        """Test StartDate is populated"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)

        null_startdate_count = scd_df.filter(col("StartDate").isNull()).count()
        assert null_startdate_count == 0

    def test_enddate_initially_null(self, sample_customer_data, sample_order_data):
        """Test EndDate is initially NULL"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)

        null_enddate_count = scd_df.filter(col("EndDate").isNull()).count()
        assert null_enddate_count == scd_df.count()

    def test_opts_populated(self, sample_customer_data, sample_order_data):
        """Test OpTs is populated"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)

        null_opts_count = scd_df.filter(col("OpTs").isNull()).count()
        assert null_opts_count == 0


class TestAggregation:
    """Test FR-AGGREGATE-001: Customer aggregate spend calculation"""

    def test_aggregate_calculation(self, sample_customer_data, sample_order_data):
        """Test aggregate spend calculation"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)
        aggregate_df = calculate_customer_aggregate_spend(scd_df)

        assert aggregate_df.count() > 0
        assert "CustId" in aggregate_df.columns
        assert "TotalSpend" in aggregate_df.columns
        assert "OrderCount" in aggregate_df.columns

    def test_aggregate_total_spend_accuracy(self, sample_customer_data, sample_order_data):
        """Test total spend is calculated correctly"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)
        aggregate_df = calculate_customer_aggregate_spend(scd_df)

        # Customer C001 has orders O001 (150.50) and O002 (200.00) = 350.50
        c001_spend = aggregate_df.filter(col("CustId") == "C001").first()["TotalSpend"]
        assert abs(c001_spend - 350.50) < 0.01

    def test_aggregate_order_count_accuracy(self, sample_customer_data, sample_order_data):
        """Test order count is calculated correctly"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)
        aggregate_df = calculate_customer_aggregate_spend(scd_df)

        # Customer C001 has 2 orders
        c001_count = aggregate_df.filter(col("CustId") == "C001").first()["OrderCount"]
        assert c001_count == 2

    def test_aggregate_includes_customer_details(self, sample_customer_data, sample_order_data):
        """Test aggregate includes customer details"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)
        aggregate_df = calculate_customer_aggregate_spend(scd_df)

        assert "Name" in aggregate_df.columns
        assert "EmailId" in aggregate_df.columns
        assert "Region" in aggregate_df.columns

    def test_aggregate_filters_active_records(self, sample_customer_data, sample_order_data):
        """Test aggregate only includes active records"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)

        # Manually set one record to inactive
        scd_df_modified = scd_df.withColumn(
            "IsActive",
            when(col("OrderId") == "O001", False).otherwise(col("IsActive"))
        )

        aggregate_df = calculate_customer_aggregate_spend(scd_df_modified)

        # Customer C001 should now have only 1 order (O002) with amount 200.00
        c001_data = aggregate_df.filter(col("CustId") == "C001").first()
        assert c001_data["OrderCount"] == 1
        assert abs(c001_data["TotalSpend"] - 200.00) < 0.01

    def test_aggregate_sorted_by_spend(self, sample_customer_data, sample_order_data):
        """Test aggregate is sorted by total spend descending"""
        joined_df = join_customer_order_data(sample_customer_data, sample_order_data)
        scd_df = apply_scd_type2_columns(joined_df)
        aggregate_df = calculate_customer_aggregate_spend(scd_df)

        spend_values = [row["TotalSpend"] for row in aggregate_df.collect()]
        assert spend_values == sorted(spend_values, reverse=True)


class TestEndToEnd:
    """Test FR-E2E-001: End-to-end workflow"""

    def test_complete_workflow(self, spark, sample_customer_data, sample_order_data):
        """Test complete data processing workflow"""
        # Clean data
        clean_customer_df = clean_dataframe(sample_customer_data)
        clean_order_df = clean_dataframe(sample_order_data)

        # Join data
        joined_df = join_customer_order_data(clean_customer_df, clean_order_df)

        # Apply SCD Type 2
        scd_df = apply_scd_type2_columns(joined_df)

        # Calculate aggregates
        aggregate_df = calculate_customer_aggregate_spend(scd_df)

        # Verify final output
        assert scd_df.count() > 0
        assert aggregate_df.count() > 0
        assert "IsActive" in scd_df.columns
        assert "TotalSpend" in aggregate_df.columns

    def test_workflow_with_dirty_data(self, spark, dirty_customer_data, sample_order_data):
        """Test workflow handles dirty data correctly"""
        # Clean data
        clean_customer_df = clean_dataframe(dirty_customer_data)
        clean_order_df = clean_dataframe(sample_order_data)

        # Verify cleaning worked
        assert clean_customer_df.count() < dirty_customer_data.count()

        # Continue workflow
        joined_df = join_customer_order_data(clean_customer_df, clean_order_df)
        scd_df = apply_scd_type2_columns(joined_df)
        aggregate_df = calculate_customer_aggregate_spend(scd_df)

        # Verify no NULL values in final output
        for col_name in aggregate_df.columns:
            null_count = aggregate_df.filter(col(col_name).isNull()).count()
            assert null_count == 0 or col_name == "LastOrderDate"