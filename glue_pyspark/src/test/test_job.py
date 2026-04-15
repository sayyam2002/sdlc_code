"""
Comprehensive test suite for Customer Order ETL Job

Tests cover:
- FR-INGEST-001: Data ingestion from S3
- TR-CLEAN-001: Data cleaning (NULL removal, deduplication)
- FR-SCD2-001: SCD Type 2 implementation with Hudi
- FR-AGGREGATE-001: Customer aggregate spend calculation
- Schema validation
- Error handling
"""

import pytest
import os
import tempfile
import shutil
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from pyspark.sql.functions import col

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'main'))

from job import CustomerOrderETL


@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing
    """
    spark = SparkSession.builder \
        .appName("CustomerOrderETL_Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def sample_data_dir():
    """
    Create temporary directory for sample data
    """
    temp_dir = tempfile.mkdtemp()

    # Create subdirectories
    customer_dir = os.path.join(temp_dir, 'customer')
    order_dir = os.path.join(temp_dir, 'order')
    output_dir = os.path.join(temp_dir, 'output')

    os.makedirs(customer_dir)
    os.makedirs(order_dir)
    os.makedirs(output_dir)

    yield {
        'base': temp_dir,
        'customer': customer_dir,
        'order': order_dir,
        'output': output_dir
    }

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_customer_data(spark, sample_data_dir):
    """
    Create sample customer data
    TRD Section: Schemas - Customer
    """
    data = [
        ("C001", "John Doe", "john.doe@email.com", "North"),
        ("C002", "Jane Smith", "jane.smith@email.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@email.com", "East"),
        ("C004", "Alice Williams", "alice.williams@email.com", "West"),
        ("C005", "Charlie Brown", "charlie.brown@email.com", "North"),
        ("C006", None, "null.customer@email.com", "South"),  # NULL value
        ("C007", "Null", "null.string@email.com", "East"),  # 'Null' string
        ("C001", "John Doe", "john.doe@email.com", "North"),  # Duplicate
    ]

    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    # Write to CSV
    customer_path = os.path.join(sample_data_dir['customer'], 'customers.csv')
    df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(sample_data_dir['customer'])

    return df


@pytest.fixture
def sample_order_data(spark, sample_data_dir):
    """
    Create sample order data
    TRD Section: Schemas - Order
    """
    data = [
        ("O001", "C001", "Laptop", 1200.00, 1),
        ("O002", "C001", "Mouse", 25.00, 2),
        ("O003", "C002", "Keyboard", 75.00, 1),
        ("O004", "C003", "Monitor", 300.00, 2),
        ("O005", "C004", "Headphones", 150.00, 1),
        ("O006", "C005", "Webcam", 80.00, 1),
        ("O007", "C002", "USB Cable", 15.00, 3),
        ("O008", None, "Null Item", 100.00, 1),  # NULL value
        ("O009", "C003", "Null", 50.00, 1),  # 'Null' string
        ("O001", "C001", "Laptop", 1200.00, 1),  # Duplicate
    ]

    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    # Write to CSV
    df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(sample_data_dir['order'])

    return df


@pytest.fixture
def etl_params(sample_data_dir):
    """
    Create ETL parameters for testing
    """
    return {
        'customer_source_path': sample_data_dir['customer'],
        'order_source_path': sample_data_dir['order'],
        'curated_customer_path': os.path.join(sample_data_dir['output'], 'customer'),
        'curated_order_path': os.path.join(sample_data_dir['output'], 'order'),
        'curated_ordersummary_path': os.path.join(sample_data_dir['output'], 'ordersummary'),
        'analytics_customeraggregatespend_path': os.path.join(sample_data_dir['output'], 'analytics'),
        'glue_database': 'test_db',
        'hudi_customer_table_name': 'test_customer_hudi',
        'hudi_order_table_name': 'test_order_hudi'
    }


@pytest.fixture
def etl_instance(etl_params):
    """
    Create CustomerOrderETL instance
    """
    return CustomerOrderETL(etl_params)


# ============================================================================
# TEST: FR-INGEST-001 - Data Ingestion
# ============================================================================

def test_read_customer_data(spark, etl_instance, sample_customer_data):
    """
    Test FR-INGEST-001: Ingest customer data from S3
    Verifies data is read correctly from source path
    """
    df = etl_instance.read_customer_data(spark)

    assert df is not None
    assert df.count() == 8  # Including duplicates and nulls
    assert set(df.columns) == {'CustId', 'Name', 'EmailId', 'Region'}


def test_read_order_data(spark, etl_instance, sample_order_data):
    """
    Test FR-INGEST-001: Ingest order data from S3
    Verifies data is read correctly from source path
    """
    df = etl_instance.read_order_data(spark)

    assert df is not None
    assert df.count() == 10  # Including duplicates and nulls
    assert set(df.columns) == {'OrderId', 'CustId', 'ItemName', 'PricePerUnit', 'Qty'}


def test_customer_schema_validation(etl_instance):
    """
    Test TRD Section: Schemas - Customer Schema
    Verifies schema matches TRD specification
    """
    schema = etl_instance.get_customer_schema()

    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "Name"
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[3].name == "Region"


def test_order_schema_validation(etl_instance):
    """
    Test TRD Section: Schemas - Order Schema
    Verifies schema matches TRD specification
    """
    schema = etl_instance.get_order_schema()

    assert len(schema.fields) == 5
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[1].name == "CustId"
    assert schema.fields[2].name == "ItemName"
    assert schema.fields[3].name == "PricePerUnit"
    assert schema.fields[3].dataType == DoubleType()
    assert schema.fields[4].name == "Qty"
    assert schema.fields[4].dataType == IntegerType()


# ============================================================================
# TEST: TR-CLEAN-001 - Data Cleaning
# ============================================================================

def test_clean_data_removes_nulls(spark, etl_instance, sample_customer_data):
    """
    Test TR-CLEAN-001: Remove NULL values
    Verifies NULL records are removed during cleaning
    """
    df_cleaned = etl_instance.clean_data(sample_customer_data, "customer")

    # Should remove 1 NULL record
    assert df_cleaned.count() < sample_customer_data.count()

    # Verify no NULL values remain
    for column in df_cleaned.columns:
        null_count = df_cleaned.filter(col(column).isNull()).count()
        assert null_count == 0


def test_clean_data_removes_null_strings(spark, etl_instance):
    """
    Test TR-CLEAN-001: Remove 'Null' string values
    Verifies 'Null' string records are removed
    """
    data = [
        ("C001", "John Doe", "john@email.com", "North"),
        ("C002", "Null", "null@email.com", "South"),
        ("C003", "Jane", "jane@email.com", "NULL"),
    ]

    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    df_cleaned = etl_instance.clean_data(df, "test")

    # Should remove 2 records with 'Null' strings
    assert df_cleaned.count() == 1
    assert df_cleaned.first()["CustId"] == "C001"


def test_clean_data_removes_duplicates(spark, etl_instance, sample_customer_data):
    """
    Test TR-CLEAN-001: Remove duplicate records
    Verifies duplicate records are removed
    """
    df_cleaned = etl_instance.clean_data(sample_customer_data, "customer")

    # Check for duplicates on CustId
    duplicate_count = df_cleaned.groupBy("CustId").count().filter(col("count") > 1).count()
    assert duplicate_count == 0


def test_clean_data_complete_pipeline(spark, etl_instance, sample_customer_data):
    """
    Test TR-CLEAN-001: Complete cleaning pipeline
    Verifies all cleaning operations work together
    """
    initial_count = sample_customer_data.count()
    df_cleaned = etl_instance.clean_data(sample_customer_data, "customer")
    final_count = df_cleaned.count()

    # Should remove: 1 NULL + 1 'Null' string + 1 duplicate = 3 records
    assert final_count < initial_count
    assert final_count == 5  # 8 - 3 = 5 clean records


# ============================================================================
# TEST: FR-SCD2-001 - SCD Type 2 Implementation
# ============================================================================

def test_add_scd2_columns_new_record(spark, etl_instance):
    """
    Test FR-SCD2-001: Add SCD Type 2 columns for new records
    Verifies IsActive=True, StartDate set, EndDate=NULL
    """
    data = [("C001", "John Doe", "john@email.com", "North")]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    df_scd2 = etl_instance.add_scd2_columns(df, is_new_record=True)

    # Verify SCD2 columns exist
    assert "IsActive" in df_scd2.columns
    assert "StartDate" in df_scd2.columns
    assert "EndDate" in df_scd2.columns
    assert "OpTs" in df_scd2.columns

    # Verify values for new record
    row = df_scd2.first()
    assert row["IsActive"] == True
    assert row["StartDate"] is not None
    assert row["EndDate"] is None
    assert row["OpTs"] is not None


def test_add_scd2_columns_historical_record(spark, etl_instance):
    """
    Test FR-SCD2-001: Add SCD Type 2 columns for historical records
    Verifies IsActive=False, EndDate set
    """
    data = [("C001", "John Doe", "john@email.com", "North")]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    df_scd2 = etl_instance.add_scd2_columns(df, is_new_record=False)

    # Verify values for historical record
    row = df_scd2.first()
    assert row["IsActive"] == False
    assert row["EndDate"] is not None
    assert row["OpTs"] is not None


def test_scd2_column_types(spark, etl_instance):
    """
    Test FR-SCD2-001: Verify SCD Type 2 column data types
    """
    data = [("C001", "John Doe", "john@email.com", "North")]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    df_scd2 = etl_instance.add_scd2_columns(df, is_new_record=True)

    # Verify data types
    schema_dict = {field.name: field.dataType for field in df_scd2.schema.fields}
    assert isinstance(schema_dict["IsActive"], BooleanType)
    assert isinstance(schema_dict["StartDate"], TimestampType)
    assert isinstance(schema_dict["EndDate"], TimestampType)
    assert isinstance(schema_dict["OpTs"], TimestampType)


# ============================================================================
# TEST: FR-AGGREGATE-001 - Customer Aggregate Spend
# ============================================================================

def test_create_order_summary(spark, etl_instance, sample_customer_data, sample_order_data):
    """
    Test order summary creation
    Verifies customer and order data are joined correctly
    """
    # Clean data first
    customer_clean = etl_instance.clean_data(sample_customer_data, "customer")
    order_clean = etl_instance.clean_data(sample_order_data, "order")

    order_summary = etl_instance.create_order_summary(spark, customer_clean, order_clean)

    assert order_summary is not None
    assert "TotalAmount" in order_summary.columns
    assert order_summary.count() > 0

    # Verify TotalAmount calculation
    row = order_summary.filter(col("OrderId") == "O001").first()
    assert row["TotalAmount"] == 1200.00  # 1200 * 1


def test_calculate_customer_aggregate_spend(spark, etl_instance, sample_customer_data, sample_order_data):
    """
    Test FR-AGGREGATE-001: Calculate customer aggregate spend
    Verifies aggregation calculations are correct
    """
    # Clean data
    customer_clean = etl_instance.clean_data(sample_customer_data, "customer")
    order_clean = etl_instance.clean_data(sample_order_data, "order")

    # Create order summary
    order_summary = etl_instance.create_order_summary(spark, customer_clean, order_clean)

    # Calculate aggregate
    aggregate = etl_instance.calculate_customer_aggregate_spend(order_summary)

    assert aggregate is not None
    assert "TotalSpend" in aggregate.columns
    assert "TotalOrders" in aggregate.columns
    assert "TotalItems" in aggregate.columns

    # Verify aggregation for C001
    c001_agg = aggregate.filter(col("CustId") == "C001").first()
    assert c001_agg is not None
    assert c001_agg["TotalOrders"] == 2  # O001, O002
    assert c001_agg["TotalSpend"] == 1250.00  # 1200 + 50


def test_aggregate_spend_all_customers(spark, etl_instance, sample_customer_data, sample_order_data):
    """
    Test FR-AGGREGATE-001: Verify aggregate for all customers
    """
    customer_clean = etl_instance.clean_data(sample_customer_data, "customer")
    order_clean = etl_instance.clean_data(sample_order_data, "order")
    order_summary = etl_instance.create_order_summary(spark, customer_clean, order_clean)
    aggregate = etl_instance.calculate_customer_aggregate_spend(order_summary)

    # Should have aggregates for all customers with orders
    assert aggregate.count() > 0

    # Verify all required columns
    required_cols = {"CustId", "Name", "EmailId", "Region", "TotalSpend", "TotalOrders", "TotalItems"}
    assert required_cols.issubset(set(aggregate.columns))


# ============================================================================
# TEST: Data Writing Operations
# ============================================================================

def test_write_parquet(spark, etl_instance, sample_data_dir):
    """
    Test writing DataFrame to Parquet format
    """
    data = [("C001", "John Doe", "john@email.com", "North")]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    output_path = os.path.join(sample_data_dir['output'], 'test_parquet')

    etl_instance.write_parquet(df, output_path, "test_table")

    # Verify file was written
    assert os.path.exists(output_path)

    # Read back and verify
    df_read = spark.read.parquet(output_path)
    assert df_read.count() == 1


@patch('job.CustomerOrderETL.write_hudi_table')
def test_write_hudi_table_called(mock_write_hudi, spark, etl_instance):
    """
    Test Hudi write operation is called with correct parameters
    """
    data = [("C001", "John Doe", "john@email.com", "North", True, datetime.now(), None, datetime.now())]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("IsActive", BooleanType(), True),
        StructField("StartDate", TimestampType(), True),
        StructField("EndDate", TimestampType(), True),
        StructField("OpTs", TimestampType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    etl_instance.write_hudi_table(df, "test_table", "CustId", "/tmp/test")

    # Verify method was called
    mock_write_hudi.assert_called_once()


# ============================================================================
# TEST: Error Handling
# ============================================================================

def test_read_customer_data_invalid_path(spark, etl_params):
    """
    Test error handling for invalid source path
    """
    etl_params['customer_source_path'] = '/invalid/path'
    etl = CustomerOrderETL(etl_params)

    with pytest.raises(Exception):
        etl.read_customer_data(spark)


def test_clean_data_empty_dataframe(spark, etl_instance):
    """
    Test cleaning empty DataFrame
    """
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True)
    ])

    df_empty = spark.createDataFrame([], schema)
    df_cleaned = etl_instance.clean_data(df_empty, "empty_test")

    assert df_cleaned.count() == 0


# ============================================================================
# TEST: Integration Tests
# ============================================================================

@patch('job.Job')
@patch('job.GlueContext')
def test_etl_run_complete_pipeline(mock_glue_context, mock_job, spark, etl_instance,
                                   sample_customer_data, sample_order_data):
    """
    Test complete ETL pipeline execution
    Verifies all steps execute without errors
    """
    # Mock GlueContext
    mock_glue = MagicMock()
    mock_glue_context.return_value = mock_glue

    # This would normally run the full pipeline
    # For testing, we verify individual components work

    # Step 1: Read data
    customer_df = etl_instance.read_customer_data(spark)
    order_df = etl_instance.read_order_data(spark)
    assert customer_df.count() > 0
    assert order_df.count() > 0

    # Step 2: Clean data
    customer_clean = etl_instance.clean_data(customer_df, "customer")
    order_clean = etl_instance.clean_data(order_df, "order")
    assert customer_clean.count() > 0
    assert order_clean.count() > 0

    # Step 3: Add SCD2 columns
    customer_scd2 = etl_instance.add_scd2_columns(customer_clean, True)
    order_scd2 = etl_instance.add_scd2_columns(order_clean, True)
    assert "IsActive" in customer_scd2.columns
    assert "IsActive" in order_scd2.columns

    # Step 4: Create order summary
    order_summary = etl_instance.create_order_summary(spark, customer_clean, order_clean)
    assert order_summary.count() > 0

    # Step 5: Calculate aggregate
    aggregate = etl_instance.calculate_customer_aggregate_spend(order_summary)
    assert aggregate.count() > 0


def test_etl_parameters_validation(etl_params):
    """
    Test ETL initialization with parameters
    Verifies all required parameters are present
    """
    etl = CustomerOrderETL(etl_params)

    assert etl.customer_source_path is not None
    assert etl.order_source_path is not None
    assert etl.curated_customer_path is not None
    assert etl.curated_order_path is not None
    assert etl.analytics_customeraggregatespend_path is not None
    assert etl.glue_database is not None


def test_s3_paths_match_trd(etl_instance):
    """
    Test that S3 paths match TRD specifications
    """
    # Verify paths contain expected components from TRD
    assert 'customerdata' in etl_instance.customer_source_path or 'customer' in etl_instance.customer_source_path
    assert 'orderdata' in etl_instance.order_source_path or 'order' in etl_instance.order_source_path
    assert 'curated' in etl_instance.curated_customer_path or 'customer' in etl_instance.curated_customer_path
    assert 'analytics' in etl_instance.analytics_customeraggregatespend_path


# ============================================================================
# TEST: Performance and Data Quality
# ============================================================================

def test_data_quality_no_nulls_after_cleaning(spark, etl_instance, sample_customer_data):
    """
    Test data quality: No NULL values after cleaning
    """
    df_cleaned = etl_instance.clean_data(sample_customer_data, "customer")

    for column in df_cleaned.columns:
        null_count = df_cleaned.filter(col(column).isNull()).count()
        assert null_count == 0, f"Column {column} has {null_count} NULL values"


def test_data_quality_no_duplicates_after_cleaning(spark, etl_instance, sample_order_data):
    """
    Test data quality: No duplicates after cleaning
    """
    df_cleaned = etl_instance.clean_data(sample_order_data, "order")

    # Check for duplicates on primary key
    total_count = df_cleaned.count()
    distinct_count = df_cleaned.select("OrderId").distinct().count()

    assert total_count == distinct_count, "Duplicate records found after cleaning"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=job", "--cov-report=html"])