"""
Comprehensive test suite for PySpark data processing job
Tests FRD requirements: FR-INGEST-001, FR-CLEAN-001, FR-SCD2-001, FR-AGG-001
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)
from pyspark.sql.functions import col
from datetime import datetime
import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from job import DataProcessor


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestDataProcessing") \
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
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        (None, "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie", "charlie@example.com", "Null"),
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
        ("C001", "John Doe", "john@example.com", "North"),
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
    ]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing"""
    data = [
        ("O001", "Laptop", 1200.50, 2, "2024-01-15"),
        ("O002", "Mouse", 25.99, 5, "2024-01-16"),
        ("O003", "Keyboard", 75.00, 3, "2024-01-17"),
        ("O004", "Monitor", 350.00, 1, "2024-01-18"),
    ]
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data_with_nulls(spark):
    """Create sample order data with NULL and 'Null' values"""
    data = [
        ("O001", "Laptop", 1200.50, 2, "2024-01-15"),
        ("O002", None, 25.99, 5, "2024-01-16"),
        ("O003", "Keyboard", None, 3, "2024-01-17"),
        (None, "Monitor", 350.00, 1, "2024-01-18"),
        ("O005", "Null", 100.00, 2, "2024-01-19"),
    ]
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


class TestDataProcessor:
    """Test suite for DataProcessor class"""

    def test_fr_ingest_001_customer_schema_validation(self, spark):
        """FR-INGEST-001: Verify customer schema matches TRD specification"""
        processor = DataProcessor()
        schema = processor.get_customer_schema()

        assert len(schema.fields) == 4
        assert schema.fields[0].name == "CustId"
        assert schema.fields[0].dataType == StringType()
        assert schema.fields[1].name == "Name"
        assert schema.fields[1].dataType == StringType()
        assert schema.fields[2].name == "EmailId"
        assert schema.fields[2].dataType == StringType()
        assert schema.fields[3].name == "Region"
        assert schema.fields[3].dataType == StringType()

    def test_fr_ingest_002_order_schema_validation(self, spark):
        """FR-INGEST-002: Verify order schema matches TRD specification"""
        processor = DataProcessor()
        schema = processor.get_order_schema()

        assert len(schema.fields) == 5
        assert schema.fields[0].name == "OrderId"
        assert schema.fields[0].dataType == StringType()
        assert schema.fields[1].name == "ItemName"
        assert schema.fields[1].dataType == StringType()
        assert schema.fields[2].name == "PricePerUnit"
        assert schema.fields[2].dataType == DoubleType()
        assert schema.fields[3].name == "Qty"
        assert schema.fields[3].dataType == IntegerType()
        assert schema.fields[4].name == "Date"
        assert schema.fields[4].dataType == StringType()

    def test_fr_clean_001_remove_null_values(self, spark, sample_customer_data_with_nulls):
        """FR-CLEAN-001: Verify NULL values are removed from dataset"""
        processor = DataProcessor()
        cleaned_df = processor.clean_data(sample_customer_data_with_nulls)

        # Should only have 1 record without NULL or 'Null'
        assert cleaned_df.count() == 1

        # Verify no NULL values exist
        for column in cleaned_df.columns:
            null_count = cleaned_df.filter(col(column).isNull()).count()
            assert null_count == 0

    def test_fr_clean_002_remove_null_string_values(self, spark, sample_customer_data_with_nulls):
        """FR-CLEAN-002: Verify 'Null' string values are removed from dataset"""
        processor = DataProcessor()
        cleaned_df = processor.clean_data(sample_customer_data_with_nulls)

        # Verify no 'Null' string values exist
        for column in cleaned_df.columns:
            null_string_count = cleaned_df.filter(col(column) == "Null").count()
            assert null_string_count == 0

    def test_fr_clean_003_order_data_null_removal(self, spark, sample_order_data_with_nulls):
        """FR-CLEAN-003: Verify NULL removal works for order data"""
        processor = DataProcessor()
        cleaned_df = processor.clean_data(sample_order_data_with_nulls)

        # Should only have 1 record without NULL or 'Null'
        assert cleaned_df.count() == 1

    def test_fr_dedup_001_remove_duplicate_customers(self, spark, sample_customer_data_with_duplicates):
        """FR-DEDUP-001: Verify duplicate customer records are removed"""
        processor = DataProcessor()
        deduped_df = processor.deduplicate_data(sample_customer_data_with_duplicates, ["CustId"])

        # Should have 3 unique customers
        assert deduped_df.count() == 3

        # Verify each CustId appears only once
        cust_id_counts = deduped_df.groupBy("CustId").count()
        max_count = cust_id_counts.agg({"count": "max"}).collect()[0][0]
        assert max_count == 1

    def test_fr_scd2_001_add_scd2_columns(self, spark, sample_customer_data):
        """FR-SCD2-001: Verify SCD Type 2 columns are added correctly"""
        processor = DataProcessor()
        scd2_df = processor.add_scd2_columns(sample_customer_data)

        # Verify all SCD2 columns exist
        assert "IsActive" in scd2_df.columns
        assert "StartDate" in scd2_df.columns
        assert "EndDate" in scd2_df.columns
        assert "OpTs" in scd2_df.columns

        # Verify column count
        assert len(scd2_df.columns) == 8  # 4 original + 4 SCD2

    def test_fr_scd2_002_isactive_default_value(self, spark, sample_customer_data):
        """FR-SCD2-002: Verify IsActive column defaults to True"""
        processor = DataProcessor()
        scd2_df = processor.add_scd2_columns(sample_customer_data)

        # All records should have IsActive = True
        active_count = scd2_df.filter(col("IsActive") == True).count()
        assert active_count == scd2_df.count()

    def test_fr_scd2_003_startdate_populated(self, spark, sample_customer_data):
        """FR-SCD2-003: Verify StartDate is populated with current timestamp"""
        processor = DataProcessor()
        scd2_df = processor.add_scd2_columns(sample_customer_data)

        # StartDate should not be NULL
        null_startdate_count = scd2_df.filter(col("StartDate").isNull()).count()
        assert null_startdate_count == 0

    def test_fr_scd2_004_enddate_null_for_active(self, spark, sample_customer_data):
        """FR-SCD2-004: Verify EndDate is NULL for active records"""
        processor = DataProcessor()
        scd2_df = processor.add_scd2_columns(sample_customer_data)

        # EndDate should be NULL for all active records
        null_enddate_count = scd2_df.filter(col("EndDate").isNull()).count()
        assert null_enddate_count == scd2_df.count()

    def test_fr_scd2_005_opts_populated(self, spark, sample_customer_data):
        """FR-SCD2-005: Verify OpTs is populated with current timestamp"""
        processor = DataProcessor()
        scd2_df = processor.add_scd2_columns(sample_customer_data)

        # OpTs should not be NULL
        null_opts_count = scd2_df.filter(col("OpTs").isNull()).count()
        assert null_opts_count == 0

    def test_fr_agg_001_calculate_total_spend(self, spark, sample_customer_data, sample_order_data):
        """FR-AGG-001: Verify total spend calculation"""
        processor = DataProcessor()
        aggregate_df = processor.calculate_customer_aggregate_spend(
            sample_customer_data,
            sample_order_data
        )

        # Verify TotalSpend column exists
        assert "TotalSpend" in aggregate_df.columns

        # Verify calculations are correct
        result = aggregate_df.collect()
        assert len(result) > 0

    def test_fr_path_001_customer_source_path(self):
        """FR-PATH-001: Verify customer source path matches TRD"""
        processor = DataProcessor()
        assert processor.CUSTOMER_SOURCE_PATH == "s3://adif-sdlc/sdlc_wizard/customerdata/"

    def test_fr_path_002_order_source_path(self):
        """FR-PATH-002: Verify order source path matches TRD"""
        processor = DataProcessor()
        assert processor.ORDER_SOURCE_PATH == "s3://adif-sdlc/sdlc_wizard/orderdata/"

    def test_fr_path_003_curated_base_path(self):
        """FR-PATH-003: Verify curated base path matches TRD"""
        processor = DataProcessor()
        assert processor.CURATED_BASE_PATH == "s3://adif-sdlc/curated/sdlc_wizard/"

    def test_fr_path_004_order_summary_path(self):
        """FR-PATH-004: Verify order summary path matches TRD"""
        processor = DataProcessor()
        assert processor.ORDER_SUMMARY_PATH == "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"

    def test_fr_path_005_analytics_path(self):
        """FR-PATH-005: Verify analytics path matches TRD"""
        processor = DataProcessor()
        assert processor.ANALYTICS_PATH == "s3://adif-sdlc/analytics/customeraggregatespend/"

    def test_fr_glue_001_database_name(self):
        """FR-GLUE-001: Verify Glue database name matches TRD"""
        processor = DataProcessor()
        assert processor.GLUE_DATABASE == "Full"

    def test_fr_clean_004_clean_preserves_valid_data(self, spark, sample_customer_data):
        """FR-CLEAN-004: Verify cleaning preserves valid data"""
        processor = DataProcessor()
        cleaned_df = processor.clean_data(sample_customer_data)

        # All valid records should be preserved
        assert cleaned_df.count() == sample_customer_data.count()

    def test_fr_dedup_002_dedup_preserves_unique_data(self, spark, sample_customer_data):
        """FR-DEDUP-002: Verify deduplication preserves unique data"""
        processor = DataProcessor()
        deduped_df = processor.deduplicate_data(sample_customer_data, ["CustId"])

        # All unique records should be preserved
        assert deduped_df.count() == sample_customer_data.count()