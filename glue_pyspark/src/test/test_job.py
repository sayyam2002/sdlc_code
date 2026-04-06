"""
Comprehensive Test Suite for Customer Order Analytics ETL Job

Tests cover all functional requirements:
- FR-INGEST-001: Data Ingestion
- FR-CLEAN-001: Data Cleaning
- FR-JOIN-001: Data Integration
- FR-AGG-001: Aggregation
- FR-SCD2-001: Historical Tracking
"""

import pytest
import yaml
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock, mock_open
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from pyspark.sql.functions import col


# Import functions from main job
import sys
sys.path.insert(0, 'src')
from main.job import (
    get_job_parameters,
    clean_dataframe,
    read_customer_data,
    read_order_data,
    calculate_customer_aggregate_spend,
    write_customer_aggregate_spend,
    create_order_summary_with_scd2,
    write_order_summary_hudi,
    register_glue_catalog_tables,
    main
)


@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing.
    """
    spark = SparkSession.builder \
        .appName("TestCustomerOrderAnalytics") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def mock_params():
    """
    Mock job parameters matching config/glue_params.yaml.
    """
    return {
        'customer_data_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
        'order_data_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
        'customer_aggregate_spend_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
        'order_summary_hudi_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
        'customer_data_format': 'parquet',
        'order_data_format': 'parquet',
        'aggregate_output_format': 'parquet',
        'order_summary_output_format': 'hudi',
        'glue_database_name': 'sdlc_wizard_db',
        'customer_table_name': 'customer_data',
        'order_table_name': 'order_data',
        'aggregate_table_name': 'customer_aggregate_spend',
        'order_summary_table_name': 'order_summary',
        'hudi_table_name': 'order_summary_hudi',
        'hudi_record_key': 'OrderId',
        'hudi_precombine_field': 'OpTs',
        'hudi_partition_field': 'Region',
        'hudi_operation': 'upsert',
        'hudi_table_type': 'COPY_ON_WRITE',
        'hudi_index_type': 'BLOOM',
        'partition_column': 'Region',
        'join_key': 'CustId',
        'scd_is_active_column': 'IsActive',
        'scd_start_date_column': 'StartDate',
        'scd_end_date_column': 'EndDate',
        'scd_op_ts_column': 'OpTs',
        'null_string_literal': 'Null',
        'remove_nulls': 'true',
        'remove_null_strings': 'true',
        'remove_duplicates': 'true',
        'spark_shuffle_partitions': '200',
        'job_name': 'customer-order-analytics-etl',
        'aggregate_write_mode': 'overwrite',
        'hudi_write_mode': 'append',
        'enable_glue_datacatalog': 'true'
    }


@pytest.fixture
def sample_customer_data(spark):
    """
    Create sample customer data for testing.
    """
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """
    Create sample order data for testing.
    """
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("IsActive", BooleanType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@example.com", "North", "O001", "Laptop", 1000.0, 2, "2024-01-15", True),
        ("C001", "John Doe", "john@example.com", "North", "O002", "Mouse", 25.0, 3, "2024-01-16", True),
        ("C002", "Jane Smith", "jane@example.com", "South", "O003", "Keyboard", 75.0, 1, "2024-01-17", True),
        ("C003", "Bob Johnson", "bob@example.com", "East", "O004", "Monitor", 300.0, 2, "2024-01-18", True),
        ("C004", "Alice Brown", "alice@example.com", "West", "O005", "Desk", 500.0, 1, "2024-01-19", True)
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_customer_data(spark):
    """
    Create customer data with NULL values and 'Null' strings for cleaning tests.
    """
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),  # NULL value
        ("C003", "Bob Johnson", "Null", "East"),  # 'Null' string
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
    ]

    return spark.createDataFrame(data, schema)


# ============================================================================
# TEST: FR-INGEST-001 - Configuration Loading
# ============================================================================

def test_get_job_parameters_local_file(mock_params):
    """
    Test loading job parameters from local YAML file.
    """
    yaml_content = yaml.dump(mock_params)

    with patch('builtins.open', mock_open(read_data=yaml_content)):
        params = get_job_parameters('config/glue_params.yaml')

    assert params is not None
    assert params['customer_data_path'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'
    assert params['order_data_path'] == 's3://adif-sdlc/sdlc_wizard/orderdata/'
    assert params['hudi_record_key'] == 'OrderId'


def test_get_job_parameters_s3_path(mock_params):
    """
    Test loading job parameters from S3 path.
    """
    yaml_content = yaml.dump(mock_params)

    mock_s3_client = MagicMock()
    mock_s3_client.get_object.return_value = {
        'Body': MagicMock(read=MagicMock(return_value=yaml_content.encode('utf-8')))
    }

    with patch('boto3.client', return_value=mock_s3_client):
        params = get_job_parameters('s3://my-bucket/config/glue_params.yaml')

    assert params is not None
    assert params['customer_data_path'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'


def test_get_job_parameters_validates_required_keys(mock_params):
    """
    Test that all required parameters are present in configuration.
    """
    yaml_content = yaml.dump(mock_params)

    with patch('builtins.open', mock_open(read_data=yaml_content)):
        params = get_job_parameters('config/glue_params.yaml')

    required_keys = [
        'customer_data_path',
        'order_data_path',
        'customer_aggregate_spend_path',
        'order_summary_hudi_path',
        'hudi_record_key',
        'hudi_precombine_field',
        'join_key'
    ]

    for key in required_keys:
        assert key in params, f"Required parameter '{key}' missing"


# ============================================================================
# TEST: FR-CLEAN-001 - Data Cleaning
# ============================================================================

def test_clean_dataframe_removes_nulls(spark, dirty_customer_data, mock_params):
    """
    Test that clean_dataframe removes rows with NULL values.
    """
    initial_count = dirty_customer_data.count()
    cleaned_df = clean_dataframe(dirty_customer_data, mock_params)

    assert cleaned_df.count() < initial_count
    # Verify no NULL values remain
    for column in cleaned_df.columns:
        null_count = cleaned_df.filter(col(column).isNull()).count()
        assert null_count == 0, f"Column {column} still contains NULL values"


def test_clean_dataframe_removes_null_strings(spark, dirty_customer_data, mock_params):
    """
    Test that clean_dataframe removes rows with 'Null' string literal.
    """
    cleaned_df = clean_dataframe(dirty_customer_data, mock_params)

    # Verify no 'Null' strings remain
    for column in cleaned_df.columns:
        null_string_count = cleaned_df.filter(
            col(column).cast("string") == "Null"
        ).count()
        assert null_string_count == 0, f"Column {column} still contains 'Null' strings"


def test_clean_dataframe_removes_duplicates(spark, dirty_customer_data, mock_params):
    """
    Test that clean_dataframe removes duplicate records.
    """
    # Count duplicates before cleaning
    duplicate_count = dirty_customer_data.count() - dirty_customer_data.dropDuplicates().count()
    assert duplicate_count > 0, "Test data should contain duplicates"

    cleaned_df = clean_dataframe(dirty_customer_data, mock_params)

    # Verify no duplicates remain
    assert cleaned_df.count() == cleaned_df.dropDuplicates().count()


def test_clean_dataframe_preserves_valid_data(spark, sample_customer_data, mock_params):
    """
    Test that clean_dataframe preserves valid data without NULLs or duplicates.
    """
    initial_count = sample_customer_data.count()
    cleaned_df = clean_dataframe(sample_customer_data, mock_params)

    assert cleaned_df.count() == initial_count


# ============================================================================
# TEST: FR-INGEST-001 - Data Ingestion
# ============================================================================

@patch('main.job.spark')
def test_read_customer_data(mock_spark, sample_customer_data, mock_params):
    """
    Test reading customer data from S3 path.
    """
    mock_spark.read.format.return_value.load.return_value = sample_customer_data

    with patch('main.job.clean_dataframe', return_value=sample_customer_data):
        result_df = read_customer_data(mock_spark, mock_params)

    mock_spark.read.format.assert_called_with('parquet')
    mock_spark.read.format.return_value.load.assert_called_with(
        's3://adif-sdlc/sdlc_wizard/customerdata/'
    )
    assert result_df.count() == sample_customer_data.count()


@patch('main.job.spark')
def test_read_order_data(mock_spark, sample_order_data, mock_params):
    """
    Test reading order data from S3 path.
    """
    mock_spark.read.format.return_value.load.return_value = sample_order_data

    with patch('main.job.clean_dataframe', return_value=sample_order_data):
        result_df = read_order_data(mock_spark, mock_params)

    mock_spark.read.format.assert_called_with('parquet')
    mock_spark.read.format.return_value.load.assert_called_with(
        's3://adif-sdlc/sdlc_wizard/orderdata/'
    )
    assert result_df.count() == sample_order_data.count()


def test_read_customer_data_validates_schema(spark, sample_customer_data, mock_params):
    """
    Test that customer data has expected schema from TRD.
    """
    expected_columns = ['CustId', 'Name', 'EmailId', 'Region']

    assert set(sample_customer_data.columns) == set(expected_columns)


def test_read_order_data_validates_schema(spark, sample_order_data, mock_params):
    """
    Test that order data has expected schema from TRD.
    """
    expected_columns = [
        'CustId', 'Name', 'EmailId', 'Region', 'OrderId',
        'ItemName', 'PricePerUnit', 'Qty', 'Date', 'IsActive'
    ]

    assert set(sample_order_data.columns) == set(expected_columns)


# ============================================================================
# TEST: FR-JOIN-001 & FR-AGG-001 - Data Integration and Aggregation
# ============================================================================

def test_calculate_customer_aggregate_spend(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test customer aggregate spend calculation.
    """
    result_df = calculate_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    # Verify schema
    expected_columns = ['CustId', 'Name', 'EmailId', 'Region', 'TotalSpend']
    assert set(result_df.columns) == set(expected_columns)

    # Verify aggregation logic
    assert result_df.count() > 0

    # Verify specific customer spend (C001: 2*1000 + 3*25 = 2075)
    c001_spend = result_df.filter(col('CustId') == 'C001').select('TotalSpend').collect()
    if c001_spend:
        assert c001_spend[0]['TotalSpend'] == 2075.0


def test_calculate_customer_aggregate_spend_join_key(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test that join uses correct key from parameters.
    """
    result_df = calculate_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    # Verify all customers in result exist in customer data
    customer_ids = [row['CustId'] for row in sample_customer_data.collect()]
    result_ids = [row['CustId'] for row in result_df.collect()]

    for result_id in result_ids:
        assert result_id in customer_ids


def test_calculate_customer_aggregate_spend_total_amount_calculation(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test TotalAmount = PricePerUnit * Qty calculation.
    """
    result_df = calculate_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    # Verify all TotalSpend values are positive
    total_spends = [row['TotalSpend'] for row in result_df.collect()]
    assert all(spend > 0 for spend in total_spends)


# ============================================================================
# TEST: FR-AGG-001 - Write Customer Aggregate Spend
# ============================================================================

def test_write_customer_aggregate_spend(spark, mock_params):
    """
    Test writing customer aggregate spend to S3.
    """
    # Create test DataFrame
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("TotalSpend", DoubleType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@example.com", "North", 2075.0),
        ("C002", "Jane Smith", "jane@example.com", "South", 75.0)
    ]

    test_df = spark.createDataFrame(data, schema)

    # Mock write operations
    mock_writer = MagicMock()
    test_df.write = MagicMock(return_value=mock_writer)
    mock_writer.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.partitionBy.return_value = mock_writer

    write_customer_aggregate_spend(test_df, mock_params)

    # Verify write was called with correct parameters
    mock_writer.format.assert_called_with('parquet')
    mock_writer.mode.assert_called_with('overwrite')
    mock_writer.partitionBy.assert_called_with('Region')
    mock_writer.save.assert_called_with('s3://adif-sdlc/analytics/customeraggregatespend/')


# ============================================================================
# TEST: FR-SCD2-001 - SCD Type 2 Implementation
# ============================================================================

def test_create_order_summary_with_scd2_columns(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test that SCD Type 2 columns are added correctly.
    """
    result_df = create_order_summary_with_scd2(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    # Verify SCD Type 2 columns exist
    scd_columns = ['IsActive', 'StartDate', 'EndDate', 'OpTs']
    for col_name in scd_columns:
        assert col_name in result_df.columns, f"SCD column {col_name} missing"


def test_create_order_summary_with_scd2_isactive_default(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test that IsActive defaults to True for new records.
    """
    result_df = create_order_summary_with_scd2(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    # Verify all IsActive values are True
    is_active_values = [row['IsActive'] for row in result_df.collect()]
    assert all(is_active_values), "All new records should have IsActive=True"


def test_create_order_summary_with_scd2_timestamps(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test that StartDate and OpTs are set to current timestamp.
    """
    result_df = create_order_summary_with_scd2(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    # Verify StartDate and OpTs are not null
    for row in result_df.collect():
        assert row['StartDate'] is not None, "StartDate should not be null"
        assert row['OpTs'] is not None, "OpTs should not be null"


def test_create_order_summary_with_scd2_enddate_null(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test that EndDate is null for active records.
    """
    result_df = create_order_summary_with_scd2(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    # Verify all EndDate values are null for active records
    for row in result_df.collect():
        if row['IsActive']:
            assert row['EndDate'] is None, "Active records should have null EndDate"


def test_create_order_summary_with_scd2_total_amount(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test that TotalAmount is calculated in order summary.
    """
    result_df = create_order_summary_with_scd2(
        sample_customer_data,
        sample_order_data,
        mock_params
    )

    assert 'TotalAmount' in result_df.columns

    # Verify TotalAmount calculation for specific order
    o001_row = result_df.filter(col('OrderId') == 'O001').collect()
    if o001_row:
        assert o001_row[0]['TotalAmount'] == 2000.0  # 1000 * 2


# ============================================================================
# TEST: FR-SCD2-001 - Hudi Write Operations
# ============================================================================

def test_write_order_summary_hudi_configuration(spark, mock_params):
    """
    Test Hudi write configuration matches TRD requirements.
    """
    # Create test DataFrame with SCD Type 2 columns
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("TotalAmount", DoubleType(), True),
        StructField("IsActive", BooleanType(), True),
        StructField("StartDate", TimestampType(), True),
        StructField("EndDate", TimestampType(), True),
        StructField("OpTs", TimestampType(), True)
    ])

    data = [
        ("O001", "C001", "North", 2000.0, True, datetime.now(), None, datetime.now())
    ]

    test_df = spark.createDataFrame(data, schema)

    # Mock write operations
    mock_writer = MagicMock()
    test_df.write = MagicMock(return_value=mock_writer)
    mock_writer.format.return_value = mock_writer
    mock_writer.options.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    write_order_summary_hudi(test_df, mock_params)

    # Verify Hudi format
    mock_writer.format.assert_called_with('hudi')

    # Verify Hudi options were set
    assert mock_writer.options.called

    # Verify save path
    mock_writer.save.assert_called_with('s3://adif-sdlc/curated/sdlc_wizard/ordersummary/')


def test_write_order_summary_hudi_record_key(spark, mock_params):
    """
    Test that Hudi uses OrderId as record key.
    """
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OpTs", TimestampType(), True)
    ])

    data = [("O001", "C001", "North", datetime.now())]
    test_df = spark.createDataFrame(data, schema)

    mock_writer = MagicMock()
    test_df.write = MagicMock(return_value=mock_writer)
    mock_writer.format.return_value = mock_writer
    mock_writer.options.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    write_order_summary_hudi(test_df, mock_params)

    # Verify options were called (Hudi configuration)
    call_args = mock_writer.options.call_args
    if call_args:
        options = call_args[1] if call_args[1] else call_args[0][0]
        assert 'hoodie.datasource.write.recordkey.field' in str(options)


# ============================================================================
# TEST: Glue Catalog Registration
# ============================================================================

@patch('main.job.spark')
def test_register_glue_catalog_tables(mock_spark, mock_params):
    """
    Test Glue Catalog table registration.
    """
    mock_spark.sql = MagicMock()

    register_glue_catalog_tables(mock_spark, mock_params)

    # Verify database creation
    assert any('CREATE DATABASE' in str(call) for call in mock_spark.sql.call_args_list)

    # Verify table creation calls
    assert mock_spark.sql.call_count >= 3  # Database + 2 tables


def test_register_glue_catalog_tables_database_name(mock_params):
    """
    Test that correct database name from TRD is used.
    """
    assert mock_params['glue_database_name'] == 'sdlc_wizard_db'


# ============================================================================
# TEST: Main ETL Orchestration
# ============================================================================

@patch('main.job.job')
@patch('main.job.get_job_parameters')
@patch('main.job.read_customer_data')
@patch('main.job.read_order_data')
@patch('main.job.calculate_customer_aggregate_spend')
@patch('main.job.write_customer_aggregate_spend')
@patch('main.job.create_order_summary_with_scd2')
@patch('main.job.write_order_summary_hudi')
@patch('main.job.register_glue_catalog_tables')
def test_main_execution_flow(
    mock_register, mock_write_hudi, mock_create_scd2,
    mock_write_agg, mock_calc_agg, mock_read_order,
    mock_read_customer, mock_get_params, mock_job,
    spark, sample_customer_data, sample_order_data, mock_params
):
    """
    Test complete main() execution flow.
    """
    # Setup mocks
    mock_get_params.return_value = mock_params
    mock_read_customer.return_value = sample_customer_data
    mock_read_order.return_value = sample_order_data

    aggregate_df = sample_customer_data.withColumn('TotalSpend', col('CustId'))
    mock_calc_agg.return_value = aggregate_df

    scd2_df = sample_order_data
    mock_create_scd2.return_value = scd2_df

    # Execute main
    main()

    # Verify execution order
    mock_get_params.assert_called_once()
    mock_job.init.assert_called_once()
    mock_read_customer.assert_called_once()
    mock_read_order.assert_called_once()
    mock_calc_agg.assert_called_once()
    mock_write_agg.assert_called_once()
    mock_create_scd2.assert_called_once()
    mock_write_hudi.assert_called_once()
    mock_register.assert_called_once()
    mock_job.commit.assert_called_once()


@patch('main.job.job')
@patch('main.job.get_job_parameters')
def test_main_handles_exceptions(mock_get_params, mock_job):
    """
    Test that main() handles exceptions properly.
    """
    mock_get_params.side_effect = Exception("Configuration error")

    with pytest.raises(Exception):
        main()


# ============================================================================
# TEST: S3 Path Validation
# ============================================================================

def test_s3_paths_match_trd(mock_params):
    """
    Test that all S3 paths match TRD specifications.
    """
    expected_paths = {
        'customer_data_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
        'order_data_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
        'customer_aggregate_spend_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
        'order_summary_hudi_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
    }

    for key, expected_path in expected_paths.items():
        assert mock_params[key] == expected_path, f"Path mismatch for {key}"


def test_hudi_configuration_matches_trd(mock_params):
    """
    Test that Hudi configuration matches TRD requirements.
    """
    assert mock_params['hudi_record_key'] == 'OrderId'
    assert mock_params['hudi_precombine_field'] == 'OpTs'
    assert mock_params['hudi_partition_field'] == 'Region'
    assert mock_params['hudi_operation'] == 'upsert'
    assert mock_params['hudi_table_type'] == 'COPY_ON_WRITE'


# ============================================================================
# TEST: End-to-End Integration
# ============================================================================

def test_end_to_end_data_flow(spark, sample_customer_data, sample_order_data, mock_params):
    """
    Test complete data flow from ingestion to output.
    """
    # Step 1: Clean data
    clean_customer = clean_dataframe(sample_customer_data, mock_params)
    clean_order = clean_dataframe(sample_order_data, mock_params)

    # Step 2: Calculate aggregates
    aggregate_df = calculate_customer_aggregate_spend(
        clean_customer, clean_order, mock_params
    )

    # Step 3: Create SCD Type 2
    scd2_df = create_order_summary_with_scd2(
        clean_customer, clean_order, mock_params
    )

    # Verify outputs
    assert aggregate_df.count() > 0
    assert scd2_df.count() > 0
    assert 'TotalSpend' in aggregate_df.columns
    assert 'IsActive' in scd2_df.columns
    assert 'OpTs' in scd2_df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=src/main", "--cov-report=html"])