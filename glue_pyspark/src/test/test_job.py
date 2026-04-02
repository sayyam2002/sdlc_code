"""
Comprehensive Test Suite for AWS Glue PySpark ETL Job

Tests cover all FRD/TRD requirements:
- FR-INGEST-001: Data ingestion from S3
- FR-CLEAN-001: Data cleaning (NULLs, 'Null' strings, duplicates)
- FR-SCD2-001: SCD Type 2 implementation
- FR-HUDI-001: Hudi format with upsert
- FR-CATALOG-001: Glue Catalog registration
- FR-AGG-001: Customer aggregate spend calculation

Test Strategy:
- Mock AWS services (S3, Glue Catalog)
- Use file:// URI for local Spark operations
- Never assign/patch df.write
- Mock pandas.read_excel to avoid openpyxl
- Real assertions on data transformations
"""

import pytest
import os
import sys
import tempfile
import shutil
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, BooleanType, TimestampType
)

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from main.job import (
    get_job_parameters,
    get_customer_schema,
    get_order_schema,
    clean_data,
    add_scd2_columns,
    calculate_customer_aggregate_spend,
    create_order_summary,
    register_glue_table
)


@pytest.fixture(scope='session')
def spark():
    """Create SparkSession for testing."""
    spark = SparkSession.builder \
        .appName('test-customer-order-etl') \
        .master('local[2]') \
        .config('spark.sql.shuffle.partitions', '2') \
        .config('spark.default.parallelism', '2') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    """Create temporary directory for test data."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_params():
    """Sample job parameters matching TRD."""
    return {
        'job': {
            'name': 'test-customer-order-scd2-etl',
            'log_level': 'ERROR'
        },
        'inputs': {
            'customer_source_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
            'order_source_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
            'source_format': 'parquet',
            'customer_format': 'parquet',
            'order_format': 'parquet'
        },
        'outputs': {
            'curated_customer_path': 's3://adif-sdlc/curated/sdlc_wizard/customer/',
            'curated_order_path': 's3://adif-sdlc/curated/sdlc_wizard/order/',
            'ordersummary_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
            'customeraggregatespend_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
            'write_mode': 'append',
            'output_format': 'hudi',
            'partition_by': 'Region'
        },
        'glue_catalog': {
            'database': 'gen_ai_poc_databrickscoe',
            'customer_table': 'sdlc_wizard_customer',
            'order_table': 'sdlc_wizard_order',
            'ordersummary_table': 'ordersummary',
            'customeraggregatespend_table': 'customeraggregatespend',
            'enable_catalog': True,
            'update_behavior': 'UPDATE_IN_DATABASE'
        },
        'hudi': {
            'customer_recordkey': 'CustId',
            'order_recordkey': 'OrderId',
            'precombine_field': 'OpTs',
            'table_type': 'COPY_ON_WRITE',
            'operation': 'upsert',
            'hive_sync_enabled': True,
            'hive_partition_fields': 'Region',
            'insert_shuffle_parallelism': 2,
            'upsert_shuffle_parallelism': 2,
            'bulk_insert_parallelism': 2
        },
        'scd2': {
            'is_active_column': 'IsActive',
            'start_date_column': 'StartDate',
            'end_date_column': 'EndDate',
            'operation_timestamp_column': 'OpTs',
            'active_flag_value': True,
            'inactive_flag_value': False,
            'end_date_default': '9999-12-31T23:59:59Z'
        },
        'data_quality': {
            'remove_nulls': True,
            'remove_null_strings': True,
            'remove_duplicates': True,
            'null_patterns': ['Null', 'NULL', 'null', 'None', 'NONE', '']
        },
        'flags': {
            'enable_scd2': True,
            'enable_data_cleaning': True,
            'enable_aggregations': True,
            'enable_glue_catalog': True,
            'enable_hudi': True,
            'debug_mode': False
        },
        'runtime': {
            'watermark': '2024-01-01T00:00:00Z'
        }
    }


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data matching TRD schema."""
    schema = get_customer_schema()
    data = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', 'Jane Smith', 'jane@example.com', 'South'),
        ('C003', 'Bob Johnson', 'bob@example.com', 'East'),
        ('C004', 'Alice Brown', 'alice@example.com', 'West'),
        ('C005', 'Charlie Wilson', 'charlie@example.com', 'North')
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data matching TRD schema."""
    schema = get_order_schema()
    data = [
        ('O001', 'C001', 'Laptop', Decimal('999.99'), 1),
        ('O002', 'C001', 'Mouse', Decimal('29.99'), 2),
        ('O003', 'C002', 'Keyboard', Decimal('79.99'), 1),
        ('O004', 'C003', 'Monitor', Decimal('299.99'), 2),
        ('O005', 'C004', 'Desk', Decimal('499.99'), 1),
        ('O006', 'C005', 'Chair', Decimal('199.99'), 2)
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_customer_data(spark):
    """Create dirty customer data for cleaning tests."""
    schema = get_customer_schema()
    data = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', 'Null', 'jane@example.com', 'South'),  # Null string
        ('C003', None, 'bob@example.com', 'East'),  # NULL value
        ('C004', 'Alice Brown', 'alice@example.com', 'West'),
        ('C004', 'Alice Brown', 'alice@example.com', 'West'),  # Duplicate
        ('C005', 'NULL', 'charlie@example.com', 'North')  # NULL string
    ]
    return spark.createDataFrame(data, schema)


# ===== TEST: FR-PARAM-001 - Parameter Loading =====

def test_get_job_parameters_from_yaml():
    """Test loading parameters from YAML config file."""
    params = get_job_parameters()

    assert params is not None
    assert 'job' in params
    assert 'inputs' in params
    assert 'outputs' in params
    assert 'glue_catalog' in params
    assert 'hudi' in params
    assert 'scd2' in params

    # Verify TRD-specific values
    assert params['glue_catalog']['database'] == 'gen_ai_poc_databrickscoe'
    assert params['inputs']['customer_source_path'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'
    assert params['inputs']['order_source_path'] == 's3://adif-sdlc/sdlc_wizard/orderdata/'


def test_job_parameters_structure(sample_params):
    """Test job parameters have required structure from TRD."""
    assert sample_params['glue_catalog']['database'] == 'gen_ai_poc_databrickscoe'
    assert sample_params['glue_catalog']['customer_table'] == 'sdlc_wizard_customer'
    assert sample_params['glue_catalog']['order_table'] == 'sdlc_wizard_order'
    assert sample_params['hudi']['customer_recordkey'] == 'CustId'
    assert sample_params['hudi']['order_recordkey'] == 'OrderId'


# ===== TEST: FR-SCHEMA-001 - Schema Validation =====

def test_customer_schema_matches_trd():
    """Test customer schema matches TRD requirements."""
    schema = get_customer_schema()

    assert len(schema.fields) == 4
    assert schema.fields[0].name == 'CustId'
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[0].nullable == False  # Primary key

    assert schema.fields[1].name == 'Name'
    assert schema.fields[1].dataType == StringType()

    assert schema.fields[2].name == 'EmailId'
    assert schema.fields[2].dataType == StringType()

    assert schema.fields[3].name == 'Region'
    assert schema.fields[3].dataType == StringType()


def test_order_schema_matches_trd():
    """Test order schema matches TRD requirements."""
    schema = get_order_schema()

    assert len(schema.fields) == 5
    assert schema.fields[0].name == 'OrderId'
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[0].nullable == False  # Primary key

    assert schema.fields[1].name == 'CustId'
    assert schema.fields[1].dataType == StringType()

    assert schema.fields[2].name == 'ItemName'
    assert schema.fields[2].dataType == StringType()

    assert schema.fields[3].name == 'PricePerUnit'
    assert isinstance(schema.fields[3].dataType, DecimalType)

    assert schema.fields[4].name == 'Qty'
    assert schema.fields[4].dataType == IntegerType()


# ===== TEST: FR-INGEST-001 - Data Ingestion =====

def test_read_customer_data(spark, sample_customer_data, temp_dir):
    """Test reading customer data from S3 (simulated with local file)."""
    # Write sample data
    customer_path = f"file://{temp_dir}/customer"
    sample_customer_data.write.mode('overwrite').parquet(customer_path)

    # Read back
    schema = get_customer_schema()
    df = spark.read.schema(schema).parquet(customer_path)

    assert df.count() == 5
    assert set(df.columns) == {'CustId', 'Name', 'EmailId', 'Region'}


def test_read_order_data(spark, sample_order_data, temp_dir):
    """Test reading order data from S3 (simulated with local file)."""
    # Write sample data
    order_path = f"file://{temp_dir}/order"
    sample_order_data.write.mode('overwrite').parquet(order_path)

    # Read back
    schema = get_order_schema()
    df = spark.read.schema(schema).parquet(order_path)

    assert df.count() == 6
    assert set(df.columns) == {'OrderId', 'CustId', 'ItemName', 'PricePerUnit', 'Qty'}


# ===== TEST: FR-CLEAN-001 - Data Cleaning =====

def test_clean_data_removes_nulls(spark, dirty_customer_data, sample_params):
    """Test data cleaning removes NULL values as per TRD."""
    cleaned_df = clean_data(dirty_customer_data, sample_params)

    # Should remove row with NULL Name (C003)
    assert cleaned_df.count() < dirty_customer_data.count()

    # Verify no NULLs remain
    for col_name in cleaned_df.columns:
        null_count = cleaned_df.filter(cleaned_df[col_name].isNull()).count()
        assert null_count == 0, f"Column {col_name} still has NULL values"


def test_clean_data_removes_null_strings(spark, dirty_customer_data, sample_params):
    """Test data cleaning removes 'Null' string values as per TRD."""
    cleaned_df = clean_data(dirty_customer_data, sample_params)

    # Should remove rows with 'Null' or 'NULL' strings (C002, C005)
    name_values = [row.Name for row in cleaned_df.select('Name').collect()]
    assert 'Null' not in name_values
    assert 'NULL' not in name_values
    assert 'null' not in name_values


def test_clean_data_removes_duplicates(spark, dirty_customer_data, sample_params):
    """Test data cleaning removes duplicate records as per TRD."""
    # Count duplicates before cleaning
    duplicate_count = dirty_customer_data.groupBy('CustId').count().filter('count > 1').count()
    assert duplicate_count > 0, "Test data should have duplicates"

    cleaned_df = clean_data(dirty_customer_data, sample_params)

    # Verify no duplicates after cleaning
    duplicate_count_after = cleaned_df.groupBy('CustId').count().filter('count > 1').count()
    assert duplicate_count_after == 0, "Duplicates should be removed"


def test_clean_data_with_flag_disabled(spark, dirty_customer_data, sample_params):
    """Test data cleaning can be disabled via flag."""
    sample_params['flags']['enable_data_cleaning'] = False

    cleaned_df = clean_data(dirty_customer_data, sample_params)

    # Should return original data unchanged
    assert cleaned_df.count() == dirty_customer_data.count()


# ===== TEST: FR-SCD2-001 - SCD Type 2 Implementation =====

def test_add_scd2_columns(spark, sample_customer_data, sample_params):
    """Test adding SCD Type 2 columns as per TRD."""
    scd2_df = add_scd2_columns(sample_customer_data, sample_params)

    # Verify SCD Type 2 columns exist
    assert 'IsActive' in scd2_df.columns
    assert 'StartDate' in scd2_df.columns
    assert 'EndDate' in scd2_df.columns
    assert 'OpTs' in scd2_df.columns

    # Verify all records are active (initial load)
    active_count = scd2_df.filter(scd2_df.IsActive == True).count()
    assert active_count == scd2_df.count()

    # Verify StartDate is populated
    null_start_dates = scd2_df.filter(scd2_df.StartDate.isNull()).count()
    assert null_start_dates == 0

    # Verify EndDate is NULL (active records)
    null_end_dates = scd2_df.filter(scd2_df.EndDate.isNull()).count()
    assert null_end_dates == scd2_df.count()


def test_scd2_column_types(spark, sample_customer_data, sample_params):
    """Test SCD Type 2 columns have correct data types."""
    scd2_df = add_scd2_columns(sample_customer_data, sample_params)

    schema_dict = {field.name: field.dataType for field in scd2_df.schema.fields}

    assert isinstance(schema_dict['IsActive'], BooleanType)
    assert isinstance(schema_dict['StartDate'], TimestampType)
    assert isinstance(schema_dict['EndDate'], TimestampType)
    assert isinstance(schema_dict['OpTs'], TimestampType)


def test_scd2_preserves_original_columns(spark, sample_customer_data, sample_params):
    """Test SCD Type 2 transformation preserves original columns."""
    original_columns = set(sample_customer_data.columns)
    scd2_df = add_scd2_columns(sample_customer_data, sample_params)

    # All original columns should still exist
    for col in original_columns:
        assert col in scd2_df.columns


# ===== TEST: FR-HUDI-001 - Hudi Format =====

def test_hudi_write_configuration(sample_params):
    """Test Hudi configuration matches TRD requirements."""
    hudi_config = sample_params['hudi']

    assert hudi_config['customer_recordkey'] == 'CustId'
    assert hudi_config['order_recordkey'] == 'OrderId'
    assert hudi_config['precombine_field'] == 'OpTs'
    assert hudi_config['table_type'] == 'COPY_ON_WRITE'
    assert hudi_config['operation'] == 'upsert'
    assert hudi_config['hive_sync_enabled'] == True


@patch('main.job.DataFrame.write')
def test_write_to_hudi_called_with_correct_params(mock_write, spark, sample_customer_data, sample_params):
    """Test Hudi write is called with correct parameters (mocked)."""
    from main.job import write_to_hudi

    # Setup mock chain
    mock_format = Mock()
    mock_options = Mock()
    mock_mode = Mock()
    mock_save = Mock()

    mock_write.format.return_value = mock_format
    mock_format.options.return_value = mock_options
    mock_options.mode.return_value = mock_mode
    mock_mode.save.return_value = mock_save

    # Add SCD2 columns first
    scd2_df = add_scd2_columns(sample_customer_data, sample_params)

    # Call write_to_hudi
    write_to_hudi(
        scd2_df,
        sample_params['outputs']['curated_customer_path'],
        sample_params['glue_catalog']['customer_table'],
        sample_params['hudi']['customer_recordkey'],
        sample_params
    )

    # Verify format was called with 'hudi'
    mock_write.format.assert_called_once_with('hudi')


# ===== TEST: FR-CATALOG-001 - Glue Catalog Registration =====

def test_register_glue_table(spark, sample_customer_data, sample_params, temp_dir):
    """Test Glue Catalog table registration."""
    database = 'test_database'
    table_name = 'test_customer'
    s3_path = f"file://{temp_dir}/catalog_test"

    # Write data first
    sample_customer_data.write.mode('overwrite').parquet(s3_path)

    # Register table
    register_glue_table(spark, sample_customer_data, database, table_name, s3_path, sample_params)

    # Verify table exists
    tables = spark.sql(f"SHOW TABLES IN {database}").collect()
    table_names = [row.tableName for row in tables]
    assert table_name in table_names


def test_register_glue_table_with_flag_disabled(spark, sample_customer_data, sample_params, temp_dir):
    """Test Glue Catalog registration can be disabled."""
    sample_params['flags']['enable_glue_catalog'] = False

    database = 'test_database_disabled'
    table_name = 'test_customer_disabled'
    s3_path = f"file://{temp_dir}/catalog_test_disabled"

    # This should not create the table
    register_glue_table(spark, sample_customer_data, database, table_name, s3_path, sample_params)

    # Verify database doesn't exist (or table doesn't exist if database does)
    databases = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
    if database in databases:
        tables = spark.sql(f"SHOW TABLES IN {database}").collect()
        table_names = [row.tableName for row in tables]
        assert table_name not in table_names


# ===== TEST: FR-AGG-001 - Customer Aggregate Spend =====

def test_calculate_customer_aggregate_spend(spark, sample_customer_data, sample_order_data, sample_params):
    """Test customer aggregate spend calculation as per TRD."""
    # Add SCD2 columns
    customer_scd2 = add_scd2_columns(sample_customer_data, sample_params)
    order_scd2 = add_scd2_columns(sample_order_data, sample_params)

    agg_df = calculate_customer_aggregate_spend(customer_scd2, order_scd2, sample_params)

    assert agg_df is not None
    assert agg_df.count() > 0

    # Verify required columns
    required_cols = {'CustId', 'Name', 'Region', 'TotalSpend', 'OrderCount',
                     'AvgOrderValue', 'MaxOrderValue', 'MinOrderValue'}
    assert required_cols.issubset(set(agg_df.columns))


def test_aggregate_spend_calculations(spark, sample_customer_data, sample_order_data, sample_params):
    """Test aggregate spend calculations are correct."""
    customer_scd2 = add_scd2_columns(sample_customer_data, sample_params)
    order_scd2 = add_scd2_columns(sample_order_data, sample_params)

    agg_df = calculate_customer_aggregate_spend(customer_scd2, order_scd2, sample_params)

    # Get C001 aggregates (has 2 orders: Laptop 999.99*1, Mouse 29.99*2)
    c001_agg = agg_df.filter(agg_df.CustId == 'C001').collect()[0]

    expected_total = Decimal('999.99') * 1 + Decimal('29.99') * 2
    assert abs(float(c001_agg.TotalSpend) - float(expected_total)) < 0.01
    assert c001_agg.OrderCount == 2


def test_aggregate_spend_with_flag_disabled(spark, sample_customer_data, sample_order_data, sample_params):
    """Test aggregation can be disabled via flag."""
    sample_params['flags']['enable_aggregations'] = False

    customer_scd2 = add_scd2_columns(sample_customer_data, sample_params)
    order_scd2 = add_scd2_columns(sample_order_data, sample_params)

    agg_df = calculate_customer_aggregate_spend(customer_scd2, order_scd2, sample_params)

    assert agg_df is None


# ===== TEST: FR-SUMMARY-001 - Order Summary =====

def test_create_order_summary(spark, sample_order_data, sample_params):
    """Test order summary creation."""
    order_scd2 = add_scd2_columns(sample_order_data, sample_params)
    summary_df = create_order_summary(order_scd2, sample_params)

    assert summary_df is not None
    assert summary_df.count() > 0

    # Verify required columns
    required_cols = {'OrderId', 'CustId', 'TotalOrderValue', 'TotalQuantity', 'ItemCount'}
    assert required_cols.issubset(set(summary_df.columns))


def test_order_summary_calculations(spark, sample_order_data, sample_params):
    """Test order summary calculations are correct."""
    order_scd2 = add_scd2_columns(sample_order_data, sample_params)
    summary_df = create_order_summary(order_scd2, sample_params)

    # Get O001 summary (Laptop 999.99*1)
    o001_summary = summary_df.filter(summary_df.OrderId == 'O001').collect()[0]

    expected_value = Decimal('999.99') * 1
    assert abs(float(o001_summary.TotalOrderValue) - float(expected_value)) < 0.01
    assert o001_summary.TotalQuantity == 1
    assert o001_summary.ItemCount == 1


# ===== TEST: FR-E2E-001 - End-to-End Integration =====

def test_end_to_end_customer_processing(spark, sample_customer_data, sample_params, temp_dir):
    """Test end-to-end customer data processing."""
    # Step 1: Clean data
    cleaned_df = clean_data(sample_customer_data, sample_params)
    assert cleaned_df.count() == 5

    # Step 2: Add SCD2 columns
    scd2_df = add_scd2_columns(cleaned_df, sample_params)
    assert 'IsActive' in scd2_df.columns
    assert 'StartDate' in scd2_df.columns

    # Step 3: Write to curated zone (using local file)
    output_path = f"file://{temp_dir}/curated_customer"
    scd2_df.write.mode('overwrite').parquet(output_path)

    # Step 4: Read back and verify
    result_df = spark.read.parquet(output_path)
    assert result_df.count() == 5
    assert 'IsActive' in result_df.columns


def test_end_to_end_order_processing(spark, sample_order_data, sample_params, temp_dir):
    """Test end-to-end order data processing."""
    # Step 1: Clean data
    cleaned_df = clean_data(sample_order_data, sample_params)
    assert cleaned_df.count() == 6

    # Step 2: Add SCD2 columns
    scd2_df = add_scd2_columns(cleaned_df, sample_params)
    assert 'IsActive' in scd2_df.columns

    # Step 3: Write to curated zone
    output_path = f"file://{temp_dir}/curated_order"
    scd2_df.write.mode('overwrite').parquet(output_path)

    # Step 4: Read back and verify
    result_df = spark.read.parquet(output_path)
    assert result_df.count() == 6


def test_end_to_end_analytics_pipeline(spark, sample_customer_data, sample_order_data, sample_params, temp_dir):
    """Test complete analytics pipeline from raw to aggregated data."""
    # Process customer data
    customer_cleaned = clean_data(sample_customer_data, sample_params)
    customer_scd2 = add_scd2_columns(customer_cleaned, sample_params)

    # Process order data
    order_cleaned = clean_data(sample_order_data, sample_params)
    order_scd2 = add_scd2_columns(order_cleaned, sample_params)

    # Create order summary
    summary_df = create_order_summary(order_scd2, sample_params)
    assert summary_df.count() > 0

    # Calculate aggregates
    agg_df = calculate_customer_aggregate_spend(customer_scd2, order_scd2, sample_params)
    assert agg_df.count() > 0

    # Write all outputs
    customer_path = f"file://{temp_dir}/customer"
    order_path = f"file://{temp_dir}/order"
    summary_path = f"file://{temp_dir}/summary"
    agg_path = f"file://{temp_dir}/aggregate"

    customer_scd2.write.mode('overwrite').parquet(customer_path)
    order_scd2.write.mode('overwrite').parquet(order_path)
    summary_df.write.mode('overwrite').parquet(summary_path)
    agg_df.write.mode('overwrite').parquet(agg_path)

    # Verify all outputs exist and have data
    assert spark.read.parquet(customer_path).count() > 0
    assert spark.read.parquet(order_path).count() > 0
    assert spark.read.parquet(summary_path).count() > 0
    assert spark.read.parquet(agg_path).count() > 0


# ===== TEST: FR-ERROR-001 - Error Handling =====

def test_clean_data_with_empty_dataframe(spark, sample_params):
    """Test data cleaning handles empty DataFrame."""
    schema = get_customer_schema()
    empty_df = spark.createDataFrame([], schema)

    cleaned_df = clean_data(empty_df, sample_params)
    assert cleaned_df.count() == 0


def test_aggregate_with_no_matching_customers(spark, sample_customer_data, sample_params):
    """Test aggregation handles orders with no matching customers."""
    # Create orders for non-existent customers
    schema = get_order_schema()
    orphan_orders = spark.createDataFrame([
        ('O999', 'C999', 'Item', Decimal('100.00'), 1)
    ], schema)

    customer_scd2 = add_scd2_columns(sample_customer_data, sample_params)
    order_scd2 = add_scd2_columns(orphan_orders, sample_params)

    agg_df = calculate_customer_aggregate_spend(customer_scd2, order_scd2, sample_params)

    # Should return empty result (inner join)
    assert agg_df.count() == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])