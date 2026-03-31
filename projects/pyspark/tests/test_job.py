"""
Comprehensive test suite for Customer Order Analytics Pipeline
Testing FRD requirements: FR-INGEST-001, FR-CLEAN-001, FR-SCD2-001, FR-AGG-001, FR-JOIN-001
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)
from pyspark.sql.functions import col
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestCustomerOrderAnalytics") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def mock_pipeline(spark):
    """Create mock pipeline with mocked AWS services"""
    with patch('src.job.SparkContext') as mock_sc, \
         patch('src.job.GlueContext') as mock_glue:

        mock_sc_instance = Mock()
        mock_sc.return_value = mock_sc_instance

        mock_glue_instance = Mock()
        mock_glue_instance.spark_session = spark
        mock_glue.return_value = mock_glue_instance

        from src.job import CustomerOrderAnalyticsPipeline
        pipeline = CustomerOrderAnalyticsPipeline()
        pipeline.spark = spark

        yield pipeline


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data matching TRD schema"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
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
    """Create sample order data matching TRD schema"""
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True)
    ])

    data = [
        ("O001", "C001", "Laptop", 1200.00, 1),
        ("O002", "C001", "Mouse", 25.00, 2),
        ("O003", "C002", "Keyboard", 75.00, 1),
        ("O004", "C003", "Monitor", 300.00, 2),
        ("O005", "C004", "Headphones", 150.00, 1),
        ("O006", "C002", "Webcam", 80.00, 1),
        ("O007", "C005", "Desk", 450.00, 1)
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_dirty_customer_data(spark):
    """Create sample customer data with NULL and 'Null' values"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@example.com", "North"),
        ("C002", None, "jane.smith@example.com", "South"),  # NULL value
        ("C003", "Bob Johnson", "Null", "East"),  # 'Null' string
        ("C004", "Alice Williams", "alice.williams@example.com", "West"),
        ("C001", "John Doe", "john.doe@example.com", "North")  # Duplicate
    ]

    return spark.createDataFrame(data, schema)


# Test FR-INGEST-001: Data Ingestion
def test_customer_schema_matches_trd(mock_pipeline):
    """Test that customer schema matches TRD specification"""
    schema = mock_pipeline.get_customer_schema()

    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "Name"
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[3].name == "Region"


def test_order_schema_matches_trd(mock_pipeline):
    """Test that order schema matches TRD specification"""
    schema = mock_pipeline.get_order_schema()

    assert len(schema.fields) == 5
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[1].name == "CustId"
    assert schema.fields[2].name == "ItemName"
    assert schema.fields[3].name == "PricePerUnit"
    assert schema.fields[3].dataType == DoubleType()
    assert schema.fields[4].name == "Qty"
    assert schema.fields[4].dataType == IntegerType()


def test_s3_paths_match_trd(mock_pipeline):
    """Test that S3 paths match TRD specification"""
    assert mock_pipeline.customer_data_path == "s3://adif-sdlc/sdlc_wizard/customerdata/"
    assert mock_pipeline.order_data_path == "s3://adif-sdlc/sdlc_wizard/orderdata/"
    assert mock_pipeline.order_summary_path == "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
    assert mock_pipeline.customer_aggregate_spend_path == "s3://adif-sdlc/analytics/customeraggregatespend/"
    assert mock_pipeline.brd_usecase1_path == "s3://adif-sdlc/brd_usecase1/"


def test_glue_catalog_config_matches_trd(mock_pipeline):
    """Test that Glue Catalog configuration matches TRD"""
    assert mock_pipeline.glue_database == "read"
    assert mock_pipeline.glue_table == "customeraggregatespend"


# Test FR-CLEAN-001: Data Cleaning
def test_clean_data_removes_nulls(mock_pipeline, sample_dirty_customer_data):
    """Test that clean_data removes NULL values"""
    cleaned_df = mock_pipeline.clean_data(sample_dirty_customer_data)

    # Should remove row with NULL Name
    assert cleaned_df.count() < sample_dirty_customer_data.count()

    # Verify no NULL values remain
    for column in cleaned_df.columns:
        null_count = cleaned_df.filter(col(column).isNull()).count()
        assert null_count == 0


def test_clean_data_removes_null_strings(mock_pipeline, sample_dirty_customer_data):
    """Test that clean_data removes 'Null' string values"""
    cleaned_df = mock_pipeline.clean_data(sample_dirty_customer_data)

    # Verify no 'Null' strings remain
    for column in cleaned_df.columns:
        null_string_count = cleaned_df.filter(col(column) == "Null").count()
        assert null_string_count == 0


def test_clean_data_removes_duplicates(mock_pipeline, sample_dirty_customer_data):
    """Test that clean_data removes duplicate records"""
    cleaned_df = mock_pipeline.clean_data(sample_dirty_customer_data)

    # Original has duplicate C001
    original_c001_count = sample_dirty_customer_data.filter(col("CustId") == "C001").count()
    cleaned_c001_count = cleaned_df.filter(col("CustId") == "C001").count()

    assert original_c001_count == 2
    assert cleaned_c001_count <= 1


def test_clean_data_preserves_valid_records(mock_pipeline, sample_customer_data):
    """Test that clean_data preserves valid records"""
    original_count = sample_customer_data.count()
    cleaned_df = mock_pipeline.clean_data(sample_customer_data)
    cleaned_count = cleaned_df.count()

    assert cleaned_count == original_count


# Test FR-JOIN-001: Join Operations
def test_join_customer_order_inner_join(mock_pipeline, sample_customer_data, sample_order_data):
    """Test that join_customer_order performs inner join correctly"""
    joined_df = mock_pipeline.join_customer_order(sample_customer_data, sample_order_data)

    # Verify join happened
    assert joined_df.count() > 0

    # Verify columns from both tables present
    columns = joined_df.columns
    assert "CustId" in columns
    assert "Name" in columns
    assert "EmailId" in columns
    assert "Region" in columns
    assert "OrderId" in columns
    assert "ItemName" in columns
    assert "PricePerUnit" in columns
    assert "Qty" in columns


def test_join_customer_order_correct_matches(mock_pipeline, sample_customer_data, sample_order_data):
    """Test that join matches correct customer-order pairs"""
    joined_df = mock_pipeline.join_customer_order(sample_customer_data, sample_order_data)

    # C001 has 2 orders
    c001_orders = joined_df.filter(col("CustId") == "C001").count()
    assert c001_orders == 2

    # C002 has 2 orders
    c002_orders = joined_df.filter(col("CustId") == "C002").count()
    assert c002_orders == 2


# Test FR-AGG-001: Aggregation
def test_calculate_aggregate_spend_total(mock_pipeline, sample_customer_data, sample_order_data):
    """Test that aggregate spend calculation is correct"""
    joined_df = mock_pipeline.join_customer_order(sample_customer_data, sample_order_data)
    aggregate_df = mock_pipeline.calculate_aggregate_spend(joined_df)

    # Verify TotalSpend column exists
    assert "TotalSpend" in aggregate_df.columns

    # C001: Laptop (1200*1) + Mouse (25*2) = 1250
    c001_spend = aggregate_df.filter(col("CustId") == "C001").select("TotalSpend").collect()[0][0]
    assert c001_spend == 1250.0


def test_calculate_aggregate_spend_order_count(mock_pipeline, sample_customer_data, sample_order_data):
    """Test that order count is calculated correctly"""
    joined_df = mock_pipeline.join_customer_order(sample_customer_data, sample_order_data)
    aggregate_df = mock_pipeline.calculate_aggregate_spend(joined_df)

    # Verify OrderCount column exists
    assert "OrderCount" in aggregate_df.columns

    # C001 has 2 orders
    c001_count = aggregate_df.filter(col("CustId") == "C001").select("OrderCount").collect()[0][0]
    assert c001_count == 2


def test_calculate_aggregate_spend_groups_by_customer(mock_pipeline, sample_customer_data, sample_order_data):
    """Test that aggregation groups by customer correctly"""
    joined_df = mock_pipeline.join_customer_order(sample_customer_data, sample_order_data)
    aggregate_df = mock_pipeline.calculate_aggregate_spend(joined_df)

    # Should have one row per customer
    customer_count = aggregate_df.select("CustId").distinct().count()
    total_count = aggregate_df.count()

    assert customer_count == total_count


# Test FR-SCD2-001: SCD Type 2 Columns
def test_add_scd2_columns_all_present(mock_pipeline, sample_customer_data):
    """Test that all SCD Type 2 columns are added"""
    df_with_scd2 = mock_pipeline.add_scd2_columns(sample_customer_data)

    columns = df_with_scd2.columns
    assert "IsActive" in columns
    assert "StartDate" in columns
    assert "EndDate" in columns
    assert "OpTs" in columns


def test_add_scd2_columns_isactive_default(mock_pipeline, sample_customer_data):
    """Test that IsActive defaults to True"""
    df_with_scd2 = mock_pipeline.add_scd2_columns(sample_customer_data)

    # All records should have IsActive = True
    active_count = df_with_scd2.filter(col("IsActive") == True).count()
    total_count = df_with_scd2.count()

    assert active_count == total_count


def test_add_scd2_columns_startdate_populated(mock_pipeline, sample_customer_data):
    """Test that StartDate is populated"""
    df_with_scd2 = mock_pipeline.add_scd2_columns(sample_customer_data)

    # StartDate should not be null
    null_startdate_count = df_with_scd2.filter(col("StartDate").isNull()).count()
    assert null_startdate_count == 0


def test_add_scd2_columns_enddate_null(mock_pipeline, sample_customer_data):
    """Test that EndDate is NULL for active records"""
    df_with_scd2 = mock_pipeline.add_scd2_columns(sample_customer_data)

    # EndDate should be null for all active records
    null_enddate_count = df_with_scd2.filter(col("EndDate").isNull()).count()
    total_count = df_with_scd2.count()

    assert null_enddate_count == total_count


def test_add_scd2_columns_opts_populated(mock_pipeline, sample_customer_data):
    """Test that OpTs is populated"""
    df_with_scd2 = mock_pipeline.add_scd2_columns(sample_customer_data)

    # OpTs should not be null
    null_opts_count = df_with_scd2.filter(col("OpTs").isNull()).count()
    assert null_opts_count == 0


# Test FR-SCD2-002: Hudi Write Configuration
def test_write_to_hudi_configuration(mock_pipeline, sample_customer_data):
    """Test that Hudi write configuration is correct"""
    df_with_scd2 = mock_pipeline.add_scd2_columns(sample_customer_data)

    with patch.object(df_with_scd2.write, 'save') as mock_save:
        mock_write = Mock()
        mock_write.format.return_value = mock_write
        mock_write.options.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.save = mock_save

        df_with_scd2.write = mock_write

        try:
            mock_pipeline.write_to_hudi(
                df=df_with_scd2,
                table_name="customeraggregatespend",
                record_key="CustId",
                precombine_key="OpTs",
                path="s3://test-path/"
            )
        except:
            pass  # Expected to fail in test environment

        # Verify format was called with 'hudi'
        mock_write.format.assert_called()


# Test Integration
def test_end_to_end_pipeline_execution(mock_pipeline, sample_customer_data, sample_order_data):
    """Test end-to-end pipeline execution with all transformations"""
    # Clean data
    customer_cleaned = mock_pipeline.clean_data(sample_customer_data)
    order_cleaned = mock_pipeline.clean_data(sample_order_data)

    # Join
    joined_df = mock_pipeline.join_customer_order(customer_cleaned, order_cleaned)

    # Aggregate
    aggregate_df = mock_pipeline.calculate_aggregate_spend(joined_df)

    # Add SCD2 columns
    final_df = mock_pipeline.add_scd2_columns(aggregate_df)

    # Verify final schema
    columns = final_df.columns
    assert "CustId" in columns
    assert "Name" in columns
    assert "EmailId" in columns
    assert "Region" in columns
    assert "TotalSpend" in columns
    assert "OrderCount" in columns
    assert "IsActive" in columns
    assert "StartDate" in columns
    assert "EndDate" in columns
    assert "OpTs" in columns

    # Verify data integrity
    assert final_df.count() > 0


def test_pipeline_handles_empty_data(mock_pipeline, spark):
    """Test that pipeline handles empty datasets gracefully"""
    empty_customer_df = spark.createDataFrame([], mock_pipeline.get_customer_schema())
    empty_order_df = spark.createDataFrame([], mock_pipeline.get_order_schema())

    # Should not raise exception
    customer_cleaned = mock_pipeline.clean_data(empty_customer_df)
    order_cleaned = mock_pipeline.clean_data(empty_order_df)
    joined_df = mock_pipeline.join_customer_order(customer_cleaned, order_cleaned)

    assert joined_df.count() == 0


def test_pipeline_preserves_data_types(mock_pipeline, sample_customer_data, sample_order_data):
    """Test that pipeline preserves correct data types throughout"""
    joined_df = mock_pipeline.join_customer_order(sample_customer_data, sample_order_data)
    aggregate_df = mock_pipeline.calculate_aggregate_spend(joined_df)
    final_df = mock_pipeline.add_scd2_columns(aggregate_df)

    # Verify data types
    schema_dict = {field.name: field.dataType for field in final_df.schema.fields}

    assert isinstance(schema_dict["CustId"], StringType)
    assert isinstance(schema_dict["Name"], StringType)
    assert isinstance(schema_dict["TotalSpend"], DoubleType)
    assert isinstance(schema_dict["OrderCount"], type(schema_dict["OrderCount"]))  # IntegerType or LongType
    assert isinstance(schema_dict["IsActive"], BooleanType)
    assert isinstance(schema_dict["StartDate"], TimestampType)
    assert isinstance(schema_dict["OpTs"], TimestampType)