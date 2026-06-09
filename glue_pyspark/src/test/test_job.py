"""
Comprehensive tests for AWS Glue PySpark Job
Tests all transformations with mocked Spark I/O
"""

import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# Initialize Spark session for tests
spark = SparkSession.builder \
    .appName("test_job") \
    .master("local[2]") \
    .getOrCreate()


def test_get_job_parameters():
    """Test job parameter loading"""
    from main.job import get_job_parameters

    params = get_job_parameters()

    assert params is not None
    assert 'inputs_customer_source_path' in params
    assert 'inputs_order_source_path' in params
    assert 'catalog_database_name' in params
    assert params['catalog_database_name'] == 'gen_ai_poc_databrickscoe'


def test_validate_s3_path():
    """Test S3 path validation"""
    from main.job import validate_s3_path

    assert validate_s3_path('s3://bucket/path') == True
    assert validate_s3_path('s3a://bucket/path') == True
    assert validate_s3_path('/local/path') == False
    assert validate_s3_path('') == False
    assert validate_s3_path(None) == False


def test_normalize_columns():
    """Test column name normalization"""
    from main.job import normalize_columns

    # Create test DataFrame with mixed case columns
    data = [('1', 'John', 'john@example.com', 'North')]
    schema = StructType([
        StructField('CustId', StringType(), True),
        StructField('Name', StringType(), True),
        StructField('EmailId', StringType(), True),
        StructField('Region', StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)

    # Normalize columns
    normalized_df = normalize_columns(df)

    # Verify all columns are lowercase
    assert 'custid' in normalized_df.columns
    assert 'name' in normalized_df.columns
    assert 'emailid' in normalized_df.columns
    assert 'region' in normalized_df.columns
    assert 'CustId' not in normalized_df.columns


def test_apply_data_quality():
    """Test data quality rules"""
    from main.job import apply_data_quality

    # Create test data with quality issues
    data = [
        ('1', 'John', 'john@example.com', 'North'),
        ('2', 'Jane', 'Null', 'South'),
        ('3', None, 'bob@example.com', 'East'),
        ('1', 'John', 'john@example.com', 'North'),  # Duplicate
        ('4', 'Alice', 'alice@example.com', 'West')
    ]
    schema = StructType([
        StructField('custid', StringType(), True),
        StructField('name', StringType(), True),
        StructField('emailid', StringType(), True),
        StructField('region', StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)

    params = {
        'dq_remove_nulls': True,
        'dq_remove_null_strings': True,
        'dq_remove_duplicates': True
    }

    # Apply data quality
    cleaned_df = apply_data_quality(df, params)

    # Verify cleaning
    assert cleaned_df.count() == 2  # Only 2 valid unique records

    # Verify no nulls
    null_count = cleaned_df.filter(cleaned_df.name.isNull()).count()
    assert null_count == 0

    # Verify no 'Null' strings
    null_string_count = cleaned_df.filter(cleaned_df.emailid == 'Null').count()
    assert null_string_count == 0


def test_ingest_customer_data():
    """Test customer data ingestion with mocked read"""
    from main.job import ingest_customer_data

    # Create mock customer data
    customer_data = [
        ('1', 'John Doe', 'john@example.com', 'North'),
        ('2', 'Jane Smith', 'jane@example.com', 'South'),
        ('3', 'Bob Johnson', 'bob@example.com', 'East')
    ]
    customer_schema = StructType([
        StructField('CustId', StringType(), True),
        StructField('Name', StringType(), True),
        StructField('EmailId', StringType(), True),
        StructField('Region', StringType(), True)
    ])
    mock_customer_df = spark.createDataFrame(customer_data, customer_schema)

    params = {
        'inputs_customer_source_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
        'csv_header': True,
        'csv_infer_schema': True,
        'csv_delimiter': ','
    }

    # Mock the DataReader.read_csv method
    with patch('main.job.DataReader.read_csv', return_value=mock_customer_df):
        customer_df = ingest_customer_data(spark, params)

        assert customer_df is not None
        assert customer_df.count() == 3
        assert 'custid' in customer_df.columns
        assert 'name' in customer_df.columns


def test_ingest_order_data():
    """Test order data ingestion with mocked read"""
    from main.job import ingest_order_data

    # Create mock order data
    order_data = [
        ('101', 'Laptop', 1200.00, 1, '2024-01-15', '1'),
        ('102', 'Mouse', 25.00, 2, '2024-01-16', '1'),
        ('103', 'Keyboard', 75.00, 1, '2024-01-17', '2')
    ]
    order_schema = StructType([
        StructField('OrderId', StringType(), True),
        StructField('ItemName', StringType(), True),
        StructField('PricePerUnit', DoubleType(), True),
        StructField('Qty', IntegerType(), True),
        StructField('Date', StringType(), True),
        StructField('CustId', StringType(), True)
    ])
    mock_order_df = spark.createDataFrame(order_data, order_schema)

    params = {
        'inputs_order_source_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
        'csv_header': True,
        'csv_infer_schema': True,
        'csv_delimiter': ','
    }

    # Mock the DataReader.read_csv method
    with patch('main.job.DataReader.read_csv', return_value=mock_order_df):
        order_df = ingest_order_data(spark, params)

        assert order_df is not None
        assert order_df.count() == 3
        assert 'orderid' in order_df.columns
        assert 'itemname' in order_df.columns
        assert 'priceperunit' in order_df.columns


def test_clean_data():
    """Test data cleaning function"""
    from main.job import clean_data

    # Create test data
    customer_data = [
        ('1', 'John', 'john@example.com', 'North'),
        ('2', 'Jane', 'Null', 'South'),
        ('3', 'Bob', 'bob@example.com', 'East')
    ]
    customer_schema = StructType([
        StructField('custid', StringType(), True),
        StructField('name', StringType(), True),
        StructField('emailid', StringType(), True),
        StructField('region', StringType(), True)
    ])
    customer_df = spark.createDataFrame(customer_data, customer_schema)

    order_data = [
        ('101', 'Laptop', 1200.00, 1, '2024-01-15', '1'),
        ('102', 'Mouse', 25.00, 2, '2024-01-16', '1')
    ]
    order_schema = StructType([
        StructField('orderid', StringType(), True),
        StructField('itemname', StringType(), True),
        StructField('priceperunit', DoubleType(), True),
        StructField('qty', IntegerType(), True),
        StructField('date', StringType(), True),
        StructField('custid', StringType(), True)
    ])
    order_df = spark.createDataFrame(order_data, order_schema)

    params = {
        'dq_remove_nulls': True,
        'dq_remove_null_strings': True,
        'dq_remove_duplicates': True
    }

    # Clean data
    cleaned_customer_df, cleaned_order_df = clean_data(customer_df, order_df, params)

    assert cleaned_customer_df.count() == 2  # Removed 'Null' string
    assert cleaned_order_df.count() == 2


def test_register_catalog_tables():
    """Test catalog table registration with mocked write"""
    from main.job import register_catalog_tables

    # Create test data
    customer_data = [('1', 'John', 'john@example.com', 'North')]
    customer_schema = StructType([
        StructField('custid', StringType(), True),
        StructField('name', StringType(), True),
        StructField('emailid', StringType(), True),
        StructField('region', StringType(), True)
    ])
    customer_df = spark.createDataFrame(customer_data, customer_schema)

    order_data = [('101', 'Laptop', 1200.00, 1, '2024-01-15', '1')]
    order_schema = StructType([
        StructField('orderid', StringType(), True),
        StructField('itemname', StringType(), True),
        StructField('priceperunit', DoubleType(), True),
        StructField('qty', IntegerType(), True),
        StructField('date', StringType(), True),
        StructField('custid', StringType(), True)
    ])
    order_df = spark.createDataFrame(order_data, order_schema)

    params = {
        'catalog_database_name': 'gen_ai_poc_databrickscoe',
        'catalog_customer_table_name': 'customer',
        'catalog_order_table_name': 'order',
        'outputs_customer_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/customer/',
        'outputs_order_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/order/',
        'write_mode': 'overwrite'
    }

    # Mock the DataWriter methods
    with patch('main.job.DataWriter._write_parquet') as mock_write:
        register_catalog_tables(customer_df, order_df, params)

        # Verify write was called twice (customer and order)
        assert mock_write.call_count == 2


def test_implement_scd_type2():
    """Test SCD Type 2 implementation with mocked Hudi write"""
    from main.job import implement_scd_type2

    # Create test data
    order_data = [('101', 'Laptop', 1200.00, 1, '2024-01-15', '1')]
    order_schema = StructType([
        StructField('orderid', StringType(), True),
        StructField('itemname', StringType(), True),
        StructField('priceperunit', DoubleType(), True),
        StructField('qty', IntegerType(), True),
        StructField('date', StringType(), True),
        StructField('custid', StringType(), True)
    ])
    order_df = spark.createDataFrame(order_data, order_schema)

    customer_data = [('1', 'John', 'john@example.com', 'North')]
    customer_schema = StructType([
        StructField('custid', StringType(), True),
        StructField('name', StringType(), True),
        StructField('emailid', StringType(), True),
        StructField('region', StringType(), True)
    ])
    customer_df = spark.createDataFrame(customer_data, customer_schema)

    params = {
        'scd_enable': True,
        'scd_active_column': 'IsActive',
        'scd_start_date_column': 'StartDate',
        'scd_end_date_column': 'EndDate',
        'scd_operation_timestamp_column': 'OpTs',
        'hudi_table_name': 'ordersummary',
        'hudi_record_key': 'orderid',
        'hudi_precombine_field': 'OpTs',
        'hudi_partition_path': 'date',
        'hudi_operation': 'upsert',
        'hudi_table_type': 'COPY_ON_WRITE',
        'catalog_database_name': 'gen_ai_poc_databrickscoe',
        'catalog_ordersummary_table_name': 'ordersummary',
        'outputs_ordersummary_hudi_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
    }

    # Mock the DataWriter._write_hudi method
    with patch('main.job.DataWriter._write_hudi') as mock_write:
        implement_scd_type2(order_df, customer_df, params)

        # Verify Hudi write was called
        assert mock_write.call_count == 1


def test_calculate_customer_aggregate_spend():
    """Test customer aggregate spend calculation with mocked write"""
    from main.job import calculate_customer_aggregate_spend

    # Create test data
    order_data = [
        ('101', 'Laptop', 1200.00, 1, '2024-01-15', '1'),
        ('102', 'Mouse', 25.00, 2, '2024-01-16', '1'),
        ('103', 'Keyboard', 75.00, 1, '2024-01-17', '2')
    ]
    order_schema = StructType([
        StructField('orderid', StringType(), True),
        StructField('itemname', StringType(), True),
        StructField('priceperunit', DoubleType(), True),
        StructField('qty', IntegerType(), True),
        StructField('date', StringType(), True),
        StructField('custid', StringType(), True)
    ])
    order_df = spark.createDataFrame(order_data, order_schema)

    customer_data = [
        ('1', 'John', 'john@example.com', 'North'),
        ('2', 'Jane', 'jane@example.com', 'South')
    ]
    customer_schema = StructType([
        StructField('custid', StringType(), True),
        StructField('name', StringType(), True),
        StructField('emailid', StringType(), True),
        StructField('region', StringType(), True)
    ])
    customer_df = spark.createDataFrame(customer_data, customer_schema)

    params = {
        'outputs_customeraggregatespend_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
        'write_mode': 'overwrite'
    }

    # Mock the DataWriter._write_parquet method
    with patch('main.job.DataWriter._write_parquet') as mock_write:
        calculate_customer_aggregate_spend(order_df, customer_df, params)

        # Verify write was called
        assert mock_write.call_count == 1


def test_data_reader_class():
    """Test DataReader helper class"""
    from main.job import DataReader

    reader = DataReader(spark)

    # Create mock data
    mock_data = [('1', 'John', 'john@example.com', 'North')]
    mock_schema = StructType([
        StructField('custid', StringType(), True),
        StructField('name', StringType(), True),
        StructField('emailid', StringType(), True),
        StructField('region', StringType(), True)
    ])
    mock_df = spark.createDataFrame(mock_data, mock_schema)

    # Mock spark.read.csv
    with patch.object(spark.read, 'csv', return_value=mock_df):
        result_df = reader.read_csv('s3://test/path', header=True, infer_schema=True)

        assert result_df is not None
        assert result_df.count() == 1


def test_data_writer_class():
    """Test DataWriter helper class"""
    from main.job import DataWriter

    writer = DataWriter(spark)

    # Create test data
    test_data = [('1', 'John', 'john@example.com', 'North')]
    test_schema = StructType([
        StructField('custid', StringType(), True),
        StructField('name', StringType(), True),
        StructField('emailid', StringType(), True),
        StructField('region', StringType(), True)
    ])
    test_df = spark.createDataFrame(test_data, test_schema)

    # Test parquet write
    with patch.object(writer, '_write_parquet') as mock_parquet:
        writer.write_dataframe(test_df, 's3://test/path', format_type='parquet')
        assert mock_parquet.call_count == 1

    # Test CSV write
    with patch.object(writer, '_write_csv') as mock_csv:
        writer.write_dataframe(test_df, 's3://test/path', format_type='csv')
        assert mock_csv.call_count == 1

    # Test Hudi write
    with patch.object(writer, '_write_hudi') as mock_hudi:
        hudi_options = {'hoodie.table.name': 'test'}
        writer.write_dataframe(test_df, 's3://test/path', format_type='hudi', hudi_options=hudi_options)
        assert mock_hudi.call_count == 1


def test_end_to_end_workflow():
    """Test complete end-to-end workflow with all mocked I/O"""
    from main.job import (
        ingest_customer_data,
        ingest_order_data,
        clean_data,
        register_catalog_tables,
        implement_scd_type2,
        calculate_customer_aggregate_spend
    )

    # Create mock data
    customer_data = [
        ('1', 'John Doe', 'john@example.com', 'North'),
        ('2', 'Jane Smith', 'jane@example.com', 'South')
    ]
    customer_schema = StructType([
        StructField('CustId', StringType(), True),
        StructField('Name', StringType(), True),
        StructField('EmailId', StringType(), True),
        StructField('Region', StringType(), True)
    ])
    mock_customer_df = spark.createDataFrame(customer_data, customer_schema)

    order_data = [
        ('101', 'Laptop', 1200.00, 1, '2024-01-15', '1'),
        ('102', 'Mouse', 25.00, 2, '2024-01-16', '1'),
        ('103', 'Keyboard', 75.00, 1, '2024-01-17', '2')
    ]
    order_schema = StructType([
        StructField('OrderId', StringType(), True),
        StructField('ItemName', StringType(), True),
        StructField('PricePerUnit', DoubleType(), True),
        StructField('Qty', IntegerType(), True),
        StructField('Date', StringType(), True),
        StructField('CustId', StringType(), True)
    ])
    mock_order_df = spark.createDataFrame(order_data, order_schema)

    params = {
        'inputs_customer_source_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
        'inputs_order_source_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
        'outputs_customer_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/customer/',
        'outputs_order_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/order/',
        'outputs_ordersummary_hudi_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
        'outputs_customeraggregatespend_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
        'catalog_database_name': 'gen_ai_poc_databrickscoe',
        'catalog_customer_table_name': 'customer',
        'catalog_order_table_name': 'order',
        'catalog_ordersummary_table_name': 'ordersummary',
        'dq_remove_nulls': True,
        'dq_remove_null_strings': True,
        'dq_remove_duplicates': True,
        'scd_enable': True,
        'scd_active_column': 'IsActive',
        'scd_start_date_column': 'StartDate',
        'scd_end_date_column': 'EndDate',
        'scd_operation_timestamp_column': 'OpTs',
        'hudi_table_name': 'ordersummary',
        'hudi_record_key': 'orderid',
        'hudi_precombine_field': 'OpTs',
        'hudi_partition_path': 'date',
        'hudi_operation': 'upsert',
        'hudi_table_type': 'COPY_ON_WRITE',
        'csv_header': True,
        'csv_infer_schema': True,
        'csv_delimiter': ',',
        'write_mode': 'overwrite'
    }

    # Mock all I/O operations
    with patch('main.job.DataReader.read_csv') as mock_read, \
         patch('main.job.DataWriter._write_parquet') as mock_write_parquet, \
         patch('main.job.DataWriter._write_hudi') as mock_write_hudi:

        # Configure mocks
        mock_read.side_effect = [mock_customer_df, mock_order_df]

        # Execute workflow
        customer_df = ingest_customer_data(spark, params)
        order_df = ingest_order_data(spark, params)

        assert customer_df.count() == 2
        assert order_df.count() == 3

        cleaned_customer_df, cleaned_order_df = clean_data(customer_df, order_df, params)

        assert cleaned_customer_df.count() == 2
        assert cleaned_order_df.count() == 3

        register_catalog_tables(cleaned_customer_df, cleaned_order_df, params)
        implement_scd_type2(cleaned_order_df, cleaned_customer_df, params)
        calculate_customer_aggregate_spend(cleaned_order_df, cleaned_customer_df, params)

        # Verify all writes were called
        assert mock_write_parquet.call_count >= 2  # catalog + aggregate
        assert mock_write_hudi.call_count == 1  # SCD Type 2


if __name__ == '__main__':
    print("Running tests...")

    test_get_job_parameters()
    print("✓ test_get_job_parameters passed")

    test_validate_s3_path()
    print("✓ test_validate_s3_path passed")

    test_normalize_columns()
    print("✓ test_normalize_columns passed")

    test_apply_data_quality()
    print("✓ test_apply_data_quality passed")

    test_ingest_customer_data()
    print("✓ test_ingest_customer_data passed")

    test_ingest_order_data()
    print("✓ test_ingest_order_data passed")

    test_clean_data()
    print("✓ test_clean_data passed")

    test_register_catalog_tables()
    print("✓ test_register_catalog_tables passed")

    test_implement_scd_type2()
    print("✓ test_implement_scd_type2 passed")

    test_calculate_customer_aggregate_spend()
    print("✓ test_calculate_customer_aggregate_spend passed")

    test_data_reader_class()
    print("✓ test_data_reader_class passed")

    test_data_writer_class()
    print("✓ test_data_writer_class passed")

    test_end_to_end_workflow()
    print("✓ test_end_to_end_workflow passed")

    print("\n" + "="*50)
    print("All tests passed successfully!")
    print("="*50)