"""
Unit tests for AWS Glue PySpark ETL Job
Tests all transformation logic without performing real I/O operations
"""

import sys
import os
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# Initialize Spark session for testing
def get_spark():
    """Get or create Spark session for testing"""
    if hasattr(sys.modules['builtins'], 'spark'):
        return getattr(sys.modules['builtins'], 'spark')

    spark = SparkSession.builder \
        .appName("test-customer-order-etl") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    return spark


spark = get_spark()


def test_get_job_parameters():
    """Test job parameters loading"""
    from main.job import get_job_parameters

    params = get_job_parameters()

    # Verify required parameters exist
    assert 'inputs_customer_source_path' in params
    assert 'inputs_order_source_path' in params
    assert 'outputs_customer_target_path' in params
    assert 'outputs_order_target_path' in params
    assert 'outputs_ordersummary_target_path' in params
    assert 'catalog_database_name' in params
    assert 'hudi_table_name' in params

    # Verify S3 paths
    assert params['inputs_customer_source_path'].startswith('s3://')
    assert params['inputs_order_source_path'].startswith('s3://')

    print("✓ test_get_job_parameters passed")


def test_normalize_columns():
    """Test column name normalization"""
    from main.job import normalize_columns

    # Create test DataFrame with mixed case columns
    data = [('John', 'Doe', 30)]
    df = spark.createDataFrame(data, ['FirstName', 'LastName', 'Age'])

    # Normalize columns
    normalized_df = normalize_columns(df)

    # Verify all columns are lowercase
    assert normalized_df.columns == ['firstname', 'lastname', 'age']

    print("✓ test_normalize_columns passed")


def test_remove_nulls():
    """Test NULL and 'Null' string removal"""
    from main.job import remove_nulls

    # Create test DataFrame with NULLs and 'Null' strings
    data = [
        ('1', 'John', 'john@email.com', 'East'),
        ('2', 'Jane', None, 'West'),
        ('3', 'Bob', 'bob@email.com', 'Null'),
        ('4', 'Alice', 'alice@email.com', 'North'),
        (None, 'Charlie', 'charlie@email.com', 'South')
    ]
    df = spark.createDataFrame(data, ['custid', 'name', 'emailid', 'region'])

    # Remove nulls
    cleaned_df = remove_nulls(df)

    # Verify only valid records remain
    assert cleaned_df.count() == 2
    result = cleaned_df.collect()
    assert result[0]['custid'] == '1'
    assert result[1]['custid'] == '4'

    print("✓ test_remove_nulls passed")


def test_remove_duplicates():
    """Test duplicate record removal"""
    from main.job import remove_duplicates

    # Create test DataFrame with duplicates
    data = [
        ('1', 'John', 'john@email.com', 'East'),
        ('1', 'John', 'john@email.com', 'East'),
        ('2', 'Jane', 'jane@email.com', 'West'),
        ('2', 'Jane', 'jane@email.com', 'West'),
        ('3', 'Bob', 'bob@email.com', 'North')
    ]
    df = spark.createDataFrame(data, ['custid', 'name', 'emailid', 'region'])

    # Remove duplicates
    deduped_df = remove_duplicates(df)

    # Verify duplicates removed
    assert deduped_df.count() == 3

    print("✓ test_remove_duplicates passed")


def test_add_scd_columns():
    """Test SCD Type 2 column addition"""
    from main.job import add_scd_columns

    # Create test DataFrame
    data = [('1', 'John', 'john@email.com', 'East')]
    df = spark.createDataFrame(data, ['custid', 'name', 'emailid', 'region'])

    # Add SCD columns
    scd_df = add_scd_columns(df)

    # Verify SCD columns exist
    assert 'IsActive' in scd_df.columns
    assert 'StartDate' in scd_df.columns
    assert 'EndDate' in scd_df.columns
    assert 'OpTs' in scd_df.columns

    # Verify values
    result = scd_df.collect()[0]
    assert result['IsActive'] == True
    assert result['StartDate'] is not None
    assert result['EndDate'] is None
    assert result['OpTs'] is not None

    print("✓ test_add_scd_columns passed")


@patch('main.job.boto3.client')
def test_validate_s3_access(mock_boto_client):
    """Test S3 access validation"""
    from main.job import validate_s3_access

    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {'Contents': []}
    mock_boto_client.return_value = mock_s3

    # Test validation
    result = validate_s3_access('s3://adif-sdlc/sdlc_wizard/customerdata/')

    assert result == True
    mock_s3.list_objects_v2.assert_called_once()

    print("✓ test_validate_s3_access passed")


def test_data_reader_read_csv():
    """Test DataReader CSV reading with mocked Spark read"""
    from main.job import DataReader

    # Create mock data
    customer_data = [
        ('1', 'John Doe', 'john@email.com', 'East'),
        ('2', 'Jane Smith', 'jane@email.com', 'West')
    ]
    mock_df = spark.createDataFrame(customer_data, ['custid', 'name', 'emailid', 'region'])

    # Patch the internal _read_csv method
    with patch.object(DataReader, '_read_csv', return_value=mock_df):
        reader = DataReader(spark)
        df = reader.read_data_safe(
            's3://adif-sdlc/sdlc_wizard/customerdata/',
            format_type='csv',
            header='true',
            delimiter=',',
            infer_schema='true'
        )

        assert df is not None
        assert df.count() == 2
        assert 'custid' in df.columns

    print("✓ test_data_reader_read_csv passed")


def test_data_writer_write_parquet():
    """Test DataWriter Parquet writing with mocked write"""
    from main.job import DataWriter

    # Create test DataFrame
    data = [('1', 'John', 'john@email.com', 'East')]
    df = spark.createDataFrame(data, ['custid', 'name', 'emailid', 'region'])

    # Patch the internal _write_parquet method
    with patch.object(DataWriter, '_write_parquet', return_value=None):
        writer = DataWriter(spark)
        result = writer.write_data_safe(
            df,
            's3://adif-sdlc/catalog/sdlc_wizard/customer/',
            format_type='parquet',
            mode='overwrite'
        )

        assert result == True

    print("✓ test_data_writer_write_parquet passed")


def test_data_writer_write_hudi():
    """Test DataWriter Hudi writing with mocked write"""
    from main.job import DataWriter

    # Create test DataFrame with SCD columns
    data = [('1', 'John', 'john@email.com', 'East', True, datetime.now(), None, datetime.now())]
    df = spark.createDataFrame(
        data,
        ['custid', 'name', 'emailid', 'region', 'IsActive', 'StartDate', 'EndDate', 'OpTs']
    )

    hudi_options = {
        'hoodie.table.name': 'ordersummary',
        'hoodie.datasource.write.recordkey.field': 'custid',
        'hoodie.datasource.write.precombine.field': 'OpTs'
    }

    # Patch the internal _write_hudi method
    with patch.object(DataWriter, '_write_hudi', return_value=None):
        writer = DataWriter(spark)
        result = writer.write_data_safe(
            df,
            's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
            format_type='hudi',
            mode='append',
            **hudi_options
        )

        assert result == True

    print("✓ test_data_writer_write_hudi passed")


def test_complete_etl_pipeline():
    """Test complete ETL pipeline with mocked I/O"""
    from main.job import (
        normalize_columns, remove_nulls, remove_duplicates,
        add_scd_columns, DataReader, DataWriter
    )

    # Create mock customer data
    customer_data = [
        ('1', 'John Doe', 'john@email.com', 'East'),
        ('2', 'Jane Smith', 'jane@email.com', 'West'),
        ('2', 'Jane Smith', 'jane@email.com', 'West'),  # Duplicate
        ('3', 'Bob Jones', None, 'North'),  # NULL email
        ('4', 'Alice Brown', 'alice@email.com', 'Null')  # 'Null' region
    ]
    customer_df = spark.createDataFrame(
        customer_data,
        ['CustId', 'Name', 'EmailId', 'Region']
    )

    # Create mock order data
    order_data = [
        ('101', 'Widget', 10.50, 2, '2024-01-01', '1'),
        ('102', 'Gadget', 25.00, 1, '2024-01-02', '2'),
        ('102', 'Gadget', 25.00, 1, '2024-01-02', '2'),  # Duplicate
        ('103', 'Tool', 15.75, None, '2024-01-03', '1'),  # NULL qty
    ]
    order_df = spark.createDataFrame(
        order_data,
        ['OrderId', 'ItemName', 'PricePerUnit', 'Qty', 'Date', 'CustId']
    )

    # Process customer data
    customer_normalized = normalize_columns(customer_df)
    customer_clean = remove_nulls(customer_normalized)
    customer_deduped = remove_duplicates(customer_clean)

    # Process order data
    order_normalized = normalize_columns(order_df)
    order_clean = remove_nulls(order_normalized)
    order_deduped = remove_duplicates(order_clean)

    # Verify cleaning results
    assert customer_deduped.count() == 1  # Only custid=1 is valid
    assert order_deduped.count() == 1  # Only orderid=101 is valid

    # Join datasets
    joined_df = order_deduped.join(customer_deduped, on='custid', how='inner')
    assert joined_df.count() == 1

    # Add SCD columns
    final_df = add_scd_columns(joined_df)

    # Verify final structure
    assert 'IsActive' in final_df.columns
    assert 'StartDate' in final_df.columns
    assert 'EndDate' in final_df.columns
    assert 'OpTs' in final_df.columns

    result = final_df.collect()[0]
    assert result['custid'] == '1'
    assert result['orderid'] == '101'
    assert result['IsActive'] == True

    print("✓ test_complete_etl_pipeline passed")


def test_register_catalog_table():
    """Test catalog table registration"""
    from main.job import register_catalog_table

    # Create test DataFrame
    data = [('1', 'John', 'john@email.com', 'East')]
    df = spark.createDataFrame(data, ['custid', 'name', 'emailid', 'region'])

    # Mock the write operation
    with patch.object(df.write, 'saveAsTable'):
        with patch.object(spark, 'sql'):
            register_catalog_table(
                spark,
                df,
                'gen_ai_poc_databrickscoe',
                'customer',
                's3://adif-sdlc/catalog/sdlc_wizard/customer/'
            )

    print("✓ test_register_catalog_table passed")


def run_all_tests():
    """Run all test functions"""
    print("\n" + "="*60)
    print("Running AWS Glue PySpark ETL Job Tests")
    print("="*60 + "\n")

    test_get_job_parameters()
    test_normalize_columns()
    test_remove_nulls()
    test_remove_duplicates()
    test_add_scd_columns()
    test_validate_s3_access()
    test_data_reader_read_csv()
    test_data_writer_write_parquet()
    test_data_writer_write_hudi()
    test_complete_etl_pipeline()
    test_register_catalog_table()

    print("\n" + "="*60)
    print("All tests passed successfully! ✓")
    print("="*60 + "\n")


if __name__ == '__main__':
    run_all_tests()