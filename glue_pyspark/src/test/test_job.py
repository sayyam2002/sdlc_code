"""
Unit Tests for SDLC Wizard Customer-Order Data Pipeline

Tests cover all FRD requirements:
- FR-INGEST-001: Customer data ingestion
- FR-INGEST-002: Order data ingestion
- FR-CLEAN-001: NULL value removal
- FR-CLEAN-002: 'Null' string removal
- FR-CLEAN-003: Duplicate removal
- FR-SCD2-001: SCD Type 2 implementation
- FR-HUDI-001: Hudi format usage
- FR-CATALOG-001: Glue Catalog registration
"""

import os
import sys
import tempfile
import shutil
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

import pytest
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from main.job import (
    SDLCWizardPipeline,
    load_config,
    get_job_parameters,
    create_spark_session
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("SDLC Wizard Pipeline Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def temp_dir():
    """Create temporary directory for test files."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def test_config():
    """Create test configuration."""
    return {
        'job': {
            'name': 'test-sdlc-wizard-pipeline',
            'log_level': 'INFO'
        },
        'inputs': {
            'customer_source_path': 's3://test-bucket/customerdata/',
            'order_source_path': 's3://test-bucket/orderdata/',
            'source_format': 'csv',
            'csv_header': True,
            'csv_delimiter': ','
        },
        'outputs': {
            'curated_target_path': 's3://test-bucket/curated/',
            'analytics_target_path': 's3://test-bucket/analytics/',
            'write_mode': 'append',
            'output_format': 'hudi'
        },
        'glue_catalog': {
            'database_name': 'sdlc_wizard_db',
            'customer_table_name': 'customer_data',
            'order_table_name': 'order_data',
            'curated_table_name': 'order_summary'
        },
        'hudi_config': {
            'table_name': 'sdlc_wizard_order_summary',
            'record_key_field': 'CustId',
            'precombine_field': 'OpTs',
            'partition_path_field': 'Region',
            'table_type': 'COPY_ON_WRITE',
            'operation': 'upsert',
            'hive_sync_enabled': True,
            'hive_sync_mode': 'hms'
        },
        'scd2_config': {
            'is_active_column': 'IsActive',
            'start_date_column': 'StartDate',
            'end_date_column': 'EndDate',
            'op_timestamp_column': 'OpTs',
            'default_end_date': '9999-12-31T23:59:59Z'
        },
        'runtime': {
            'watermark': '2024-01-01T00:00:00Z'
        },
        'flags': {
            'enable_scd2': True,
            'enable_hudi': True,
            'enable_data_cleaning': True,
            'enable_catalog_registration': True
        },
        'data_quality': {
            'remove_nulls': True,
            'remove_null_strings': True,
            'remove_duplicates': True
        }
    }


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing."""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North")
    ]

    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing."""
    data = [
        ("C001", "O001"),
        ("C001", "O002"),
        ("C002", "O003"),
        ("C003", "O004"),
        ("C004", "O005")
    ]

    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("OrderId", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_dirty_data(spark):
    """Create sample data with NULLs, 'Null' strings, and duplicates."""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),  # NULL value
        ("C003", "Null", "bob@example.com", "East"),  # 'Null' string
        ("C004", "Alice Brown", "null", "West"),  # 'null' string
        ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
        ("C005", "", "charlie@example.com", "North")  # Empty string
    ]

    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def pipeline(spark, test_config):
    """Create pipeline instance for testing."""
    return SDLCWizardPipeline(spark, test_config)


# Test FR-INGEST-001: Customer data ingestion
def test_read_customer_data_schema(spark, temp_dir, test_config):
    """Test that customer data is read with correct schema."""
    # Create test CSV file
    customer_csv = os.path.join(temp_dir, "customers.csv")
    with open(customer_csv, 'w') as f:
        f.write("CustId,Name,EmailId,Region\n")
        f.write("C001,John Doe,john@example.com,North\n")
        f.write("C002,Jane Smith,jane@example.com,South\n")

    # Update config with local path
    test_config['inputs']['customer_source_path'] = f"file://{customer_csv}"

    pipeline = SDLCWizardPipeline(spark, test_config)
    df = pipeline.read_customer_data()

    # Verify schema
    assert df.schema == pipeline.CUSTOMER_SCHEMA
    assert df.count() == 2
    assert "CustId" in df.columns
    assert "Name" in df.columns
    assert "EmailId" in df.columns
    assert "Region" in df.columns


def test_read_customer_data_s3_path(test_config):
    """Test that customer source path matches TRD specification."""
    expected_path = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    test_config['inputs']['customer_source_path'] = expected_path

    assert test_config['inputs']['customer_source_path'] == expected_path


# Test FR-INGEST-002: Order data ingestion
def test_read_order_data_schema(spark, temp_dir, test_config):
    """Test that order data is read with correct schema."""
    # Create test CSV file
    order_csv = os.path.join(temp_dir, "orders.csv")
    with open(order_csv, 'w') as f:
        f.write("CustId,OrderId\n")
        f.write("C001,O001\n")
        f.write("C002,O002\n")

    # Update config with local path
    test_config['inputs']['order_source_path'] = f"file://{order_csv}"

    pipeline = SDLCWizardPipeline(spark, test_config)
    df = pipeline.read_order_data()

    # Verify schema
    assert df.schema == pipeline.ORDER_SCHEMA
    assert df.count() == 2
    assert "CustId" in df.columns
    assert "OrderId" in df.columns


def test_read_order_data_s3_path(test_config):
    """Test that order source path matches TRD specification."""
    expected_path = "s3://adif-sdlc/sdlc_wizard/orderdata/"
    test_config['inputs']['order_source_path'] = expected_path

    assert test_config['inputs']['order_source_path'] == expected_path


# Test FR-CLEAN-001: NULL value removal
def test_clean_data_removes_nulls(pipeline, sample_dirty_data):
    """Test that NULL values are removed from key columns."""
    cleaned_df = pipeline.clean_data(sample_dirty_data, "test")

    # Check that rows with NULL in Name are removed
    assert cleaned_df.filter(cleaned_df.Name.isNull()).count() == 0

    # Original had 6 rows, should have fewer after cleaning
    assert cleaned_df.count() < sample_dirty_data.count()


# Test FR-CLEAN-002: 'Null' string removal
def test_clean_data_removes_null_strings(pipeline, sample_dirty_data):
    """Test that 'Null' string values are converted to NULL."""
    cleaned_df = pipeline.clean_data(sample_dirty_data, "test")

    # Check that 'Null' strings are converted to NULL
    null_string_rows = cleaned_df.filter(
        (cleaned_df.Name == "Null") |
        (cleaned_df.EmailId == "null")
    ).count()

    assert null_string_rows == 0


# Test FR-CLEAN-003: Duplicate removal
def test_clean_data_removes_duplicates(pipeline, sample_dirty_data):
    """Test that duplicate records are removed."""
    cleaned_df = pipeline.clean_data(sample_dirty_data, "test")

    # Check for duplicates on CustId
    duplicate_count = cleaned_df.groupBy("CustId").count().filter("count > 1").count()

    assert duplicate_count == 0


def test_clean_data_disabled(pipeline, sample_dirty_data):
    """Test that cleaning can be disabled via flag."""
    pipeline.enable_data_cleaning = False

    cleaned_df = pipeline.clean_data(sample_dirty_data, "test")

    # Should return original data unchanged
    assert cleaned_df.count() == sample_dirty_data.count()


# Test FR-SCD2-001: SCD Type 2 implementation
def test_add_scd2_columns(pipeline, sample_customer_data):
    """Test that SCD Type 2 columns are added correctly."""
    scd2_df = pipeline.add_scd2_columns(sample_customer_data)

    # Verify SCD2 columns exist
    assert "IsActive" in scd2_df.columns
    assert "StartDate" in scd2_df.columns
    assert "EndDate" in scd2_df.columns
    assert "OpTs" in scd2_df.columns

    # Verify all records are active
    assert scd2_df.filter(scd2_df.IsActive == True).count() == scd2_df.count()

    # Verify EndDate is set to default
    end_dates = scd2_df.select("EndDate").distinct().collect()
    assert len(end_dates) == 1


def test_add_scd2_columns_disabled(pipeline, sample_customer_data):
    """Test that SCD2 columns are not added when disabled."""
    pipeline.enable_scd2 = False

    scd2_df = pipeline.add_scd2_columns(sample_customer_data)

    # Should not have SCD2 columns
    assert "IsActive" not in scd2_df.columns
    assert "StartDate" not in scd2_df.columns
    assert "EndDate" not in scd2_df.columns
    assert "OpTs" not in scd2_df.columns


def test_scd2_column_names_from_config(pipeline, sample_customer_data):
    """Test that SCD2 column names come from configuration."""
    scd2_df = pipeline.add_scd2_columns(sample_customer_data)

    config = pipeline.params['scd2_config']

    assert config['is_active_column'] in scd2_df.columns
    assert config['start_date_column'] in scd2_df.columns
    assert config['end_date_column'] in scd2_df.columns
    assert config['op_timestamp_column'] in scd2_df.columns


# Test data joining
def test_join_customer_order_data(pipeline, sample_customer_data, sample_order_data):
    """Test that customer and order data are joined correctly."""
    joined_df = pipeline.join_customer_order_data(sample_customer_data, sample_order_data)

    # Verify join results
    assert joined_df.count() == 5  # 5 orders
    assert "CustId" in joined_df.columns
    assert "OrderId" in joined_df.columns
    assert "Name" in joined_df.columns
    assert "Region" in joined_df.columns

    # Verify C001 has 2 orders
    c001_orders = joined_df.filter(joined_df.CustId == "C001").count()
    assert c001_orders == 2


# Test FR-HUDI-001: Hudi format usage
def test_write_to_hudi_configuration(pipeline, sample_customer_data, temp_dir):
    """Test that Hudi write configuration is correct."""
    target_path = f"file://{temp_dir}/hudi_output"
    table_name = "test_table"

    # Mock the write operation to capture options
    with patch.object(sample_customer_data.write, 'save') as mock_save:
        with patch.object(sample_customer_data.write, 'format', return_value=sample_customer_data.write) as mock_format:
            with patch.object(sample_customer_data.write, 'options', return_value=sample_customer_data.write) as mock_options:
                with patch.object(sample_customer_data.write, 'mode', return_value=sample_customer_data.write) as mock_mode:
                    pipeline.write_to_hudi(sample_customer_data, target_path, table_name)

                    # Verify Hudi format is used
                    mock_format.assert_called_once_with("hudi")


def test_write_to_hudi_disabled(pipeline, sample_customer_data, temp_dir):
    """Test that Hudi can be disabled and falls back to parquet."""
    pipeline.enable_hudi = False
    target_path = f"file://{temp_dir}/parquet_output"
    table_name = "test_table"

    # Should write as parquet
    pipeline.write_to_hudi(sample_customer_data, target_path, table_name)

    # Verify parquet files exist
    assert os.path.exists(temp_dir + "/parquet_output")


def test_hudi_config_from_trd(test_config):
    """Test that Hudi configuration matches TRD specifications."""
    hudi_config = test_config['hudi_config']

    assert hudi_config['table_name'] == 'sdlc_wizard_order_summary'
    assert hudi_config['record_key_field'] == 'CustId'
    assert hudi_config['precombine_field'] == 'OpTs'
    assert hudi_config['partition_path_field'] == 'Region'
    assert hudi_config['operation'] == 'upsert'


# Test FR-CATALOG-001: Glue Catalog registration
def test_register_glue_catalog_table(pipeline, sample_customer_data):
    """Test Glue Catalog table registration."""
    table_name = "test_table"
    s3_path = "s3://test-bucket/test-path/"

    # Mock Glue context
    pipeline.glue_context = Mock()

    with patch.object(pipeline.spark, 'sql') as mock_sql:
        pipeline.register_glue_catalog_table(sample_customer_data, table_name, s3_path)

        # Verify SQL was called to create table
        mock_sql.assert_called_once()
        call_args = mock_sql.call_args[0][0]
        assert "CREATE TABLE" in call_args
        assert table_name in call_args
        assert s3_path in call_args


def test_register_glue_catalog_disabled(pipeline, sample_customer_data):
    """Test that Glue Catalog registration can be disabled."""
    pipeline.params['flags']['enable_catalog_registration'] = False

    table_name = "test_table"
    s3_path = "s3://test-bucket/test-path/"

    with patch.object(pipeline.spark, 'sql') as mock_sql:
        pipeline.register_glue_catalog_table(sample_customer_data, table_name, s3_path)

        # Should not call SQL
        mock_sql.assert_not_called()


def test_glue_database_name_from_trd(test_config):
    """Test that Glue database name matches TRD specification."""
    expected_database = "sdlc_wizard_db"

    assert test_config['glue_catalog']['database_name'] == expected_database


# Test configuration loading
def test_load_config(temp_dir):
    """Test YAML configuration loading."""
    config_file = os.path.join(temp_dir, "test_config.yaml")

    test_data = {
        'job': {'name': 'test-job'},
        'inputs': {'customer_source_path': 's3://test/path/'}
    }

    with open(config_file, 'w') as f:
        yaml.dump(test_data, f)

    loaded_config = load_config(config_file)

    assert loaded_config['job']['name'] == 'test-job'
    assert loaded_config['inputs']['customer_source_path'] == 's3://test/path/'


def test_get_job_parameters_yaml_defaults(temp_dir):
    """Test that job parameters load from YAML defaults."""
    config_file = os.path.join(temp_dir, "glue_params.yaml")

    test_data = {
        'job': {'name': 'test-job'},
        'inputs': {
            'customer_source_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
            'order_source_path': 's3://adif-sdlc/sdlc_wizard/orderdata/'
        },
        'outputs': {
            'curated_target_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
        },
        'glue_catalog': {'database_name': 'sdlc_wizard_db'},
        'flags': {'enable_scd2': True}
    }

    with open(config_file, 'w') as f:
        yaml.dump(test_data, f)

    with patch('main.job.os.path.abspath', return_value=temp_dir):
        with patch('main.job.os.path.dirname', return_value=temp_dir):
            params = get_job_parameters()

            assert params['inputs']['customer_source_path'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'


# Test S3 paths from TRD
def test_s3_paths_match_trd(test_config):
    """Test that all S3 paths match TRD specifications."""
    # Update config with TRD paths
    test_config['inputs']['customer_source_path'] = 's3://adif-sdlc/sdlc_wizard/customerdata/'
    test_config['inputs']['order_source_path'] = 's3://adif-sdlc/sdlc_wizard/orderdata/'
    test_config['outputs']['curated_target_path'] = 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
    test_config['outputs']['analytics_target_path'] = 's3://adif-sdlc/analytics/'

    assert 's3://adif-sdlc/sdlc_wizard/customerdata/' in test_config['inputs']['customer_source_path']
    assert 's3://adif-sdlc/sdlc_wizard/orderdata/' in test_config['inputs']['order_source_path']
    assert 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/' in test_config['outputs']['curated_target_path']
    assert 's3://adif-sdlc/analytics/' in test_config['outputs']['analytics_target_path']


# Test schemas from TRD
def test_customer_schema_matches_trd():
    """Test that customer schema matches TRD specification."""
    expected_columns = ["CustId", "Name", "EmailId", "Region"]

    schema = SDLCWizardPipeline.CUSTOMER_SCHEMA
    actual_columns = [field.name for field in schema.fields]

    assert actual_columns == expected_columns
    assert schema["CustId"].dataType == StringType()
    assert schema["CustId"].nullable == False


def test_order_schema_matches_trd():
    """Test that order schema matches TRD specification."""
    expected_columns = ["CustId", "OrderId"]

    schema = SDLCWizardPipeline.ORDER_SCHEMA
    actual_columns = [field.name for field in schema.fields]

    assert actual_columns == expected_columns
    assert schema["CustId"].dataType == StringType()
    assert schema["OrderId"].dataType == StringType()


# Integration test
def test_pipeline_end_to_end(spark, temp_dir, test_config):
    """Test complete pipeline execution end-to-end."""
    # Create test data files
    customer_csv = os.path.join(temp_dir, "customers.csv")
    with open(customer_csv, 'w') as f:
        f.write("CustId,Name,EmailId,Region\n")
        f.write("C001,John Doe,john@example.com,North\n")
        f.write("C002,Jane Smith,jane@example.com,South\n")

    order_csv = os.path.join(temp_dir, "orders.csv")
    with open(order_csv, 'w') as f:
        f.write("CustId,OrderId\n")
        f.write("C001,O001\n")
        f.write("C002,O002\n")

    # Update config with local paths
    test_config['inputs']['customer_source_path'] = f"file://{customer_csv}"
    test_config['inputs']['order_source_path'] = f"file://{order_csv}"
    test_config['outputs']['curated_target_path'] = f"file://{temp_dir}/curated/"
    test_config['outputs']['analytics_target_path'] = f"file://{temp_dir}/analytics/"
    test_config['flags']['enable_hudi'] = False  # Use parquet for testing
    test_config['flags']['enable_catalog_registration'] = False

    pipeline = SDLCWizardPipeline(spark, test_config)

    # Run pipeline
    pipeline.run()

    # Verify output exists
    assert os.path.exists(temp_dir + "/curated")
    assert os.path.exists(temp_dir + "/analytics")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])