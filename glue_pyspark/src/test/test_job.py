"""
Unit tests for Customer Order Analytics ETL job
Tests all extraction, validation, transformation, and loading functions
"""

import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DateType

# Import functions to test
from main.job import (
    get_job_parameters,
    get_customer_schema,
    get_order_schema,
    extract_customers,
    extract_orders,
    validate_customers,
    validate_orders,
    transform_derive_full_name,
    transform_join_customer_orders,
    transform_calculate_order_count,
    transform_add_processed_date,
    load_customer_orders,
    S3DataReader,
    S3DataWriter
)


# Initialize Spark session for testing
spark = SparkSession.builder \
    .appName("CustomerOrderAnalyticsETL_Test") \
    .master("local[2]") \
    .getOrCreate()


def test_get_job_parameters():
    """Test job parameter loading"""
    params = get_job_parameters()

    assert params is not None
    assert 'inputs_customers_path' in params
    assert 'inputs_orders_path' in params
    assert 'outputs_target_path' in params
    assert params['inputs_customers_path'] == 's3://adif-sdlc/busi_req_doc/customers.csv'
    assert params['inputs_orders_path'] == 's3://adif-sdlc/busi_req_doc/orders.csv'
    print("✓ test_get_job_parameters passed")


def test_get_customer_schema():
    """Test customer schema definition"""
    schema = get_customer_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 3
    assert schema.fields[0].name == "cid"
    assert schema.fields[1].name == "fname"
    assert schema.fields[2].name == "lname"
    print("✓ test_get_customer_schema passed")


def test_get_order_schema():
    """Test order schema definition"""
    schema = get_order_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 3
    assert schema.fields[0].name == "oid"
    assert schema.fields[1].name == "cid"
    assert schema.fields[2].name == "oname"
    print("✓ test_get_order_schema passed")


def test_extract_customers():
    """Test customer data extraction with mocked S3 read"""
    params = get_job_parameters()

    # Create sample customer data
    sample_data = [
        (1, "John", "Doe"),
        (2, "Jane", "Smith"),
        (3, "Bob", "Johnson")
    ]

    mock_df = spark.createDataFrame(sample_data, ["cid", "fname", "lname"])

    # Mock the S3DataReader._read_csv method
    with patch.object(S3DataReader, '_read_csv', return_value=mock_df):
        df_result = extract_customers(spark, params)

        assert df_result is not None
        assert df_result.count() == 3
        assert "cid" in df_result.columns
        assert "fname" in df_result.columns
        assert "lname" in df_result.columns

    print("✓ test_extract_customers passed")


def test_extract_orders():
    """Test order data extraction with mocked S3 read"""
    params = get_job_parameters()

    # Create sample order data
    sample_data = [
        (101, 1, "Order_A"),
        (102, 1, "Order_B"),
        (103, 2, "Order_C")
    ]

    mock_df = spark.createDataFrame(sample_data, ["oid", "cid", "oname"])

    # Mock the S3DataReader._read_csv method
    with patch.object(S3DataReader, '_read_csv', return_value=mock_df):
        df_result = extract_orders(spark, params)

        assert df_result is not None
        assert df_result.count() == 3
        assert "oid" in df_result.columns
        assert "cid" in df_result.columns
        assert "oname" in df_result.columns

    print("✓ test_extract_orders passed")


def test_validate_customers():
    """Test customer data validation"""
    params = get_job_parameters()

    # Create sample data with valid and invalid records
    sample_data = [
        (1, "John", "Doe"),
        (2, "Jane", "Smith"),
        (None, "Invalid", "User"),  # Invalid: null CID
        (3, "", "Johnson"),  # Invalid: empty fname
        (4, "Bob", None),  # Invalid: null lname
        (5, "Alice", "Brown"),
        (5, "Alice", "Brown")  # Duplicate
    ]

    df_customers = spark.createDataFrame(sample_data, ["cid", "fname", "lname"])

    # Mock the S3DataWriter._write_csv method to avoid actual S3 writes
    with patch.object(S3DataWriter, '_write_csv', return_value=None):
        df_valid = validate_customers(spark, df_customers, params)

        assert df_valid is not None
        assert df_valid.count() == 3  # Only 3 valid unique records

        # Verify no invalid records in result
        assert df_valid.filter("cid IS NULL").count() == 0
        assert df_valid.filter("fname IS NULL OR fname = ''").count() == 0
        assert df_valid.filter("lname IS NULL OR lname = ''").count() == 0

    print("✓ test_validate_customers passed")


def test_validate_orders():
    """Test order data validation"""
    params = get_job_parameters()

    # Create sample data with valid and invalid records
    sample_data = [
        (101, 1, "Order_A"),
        (102, 1, "Order_B"),
        (None, 2, "Order_C"),  # Invalid: null OID
        (103, None, "Order_D"),  # Invalid: null CID
        (104, 2, ""),  # Invalid: empty oname
        (105, 3, "Order_E"),
        (105, 3, "Order_E")  # Duplicate
    ]

    df_orders = spark.createDataFrame(sample_data, ["oid", "cid", "oname"])

    # Mock the S3DataWriter._write_csv method
    with patch.object(S3DataWriter, '_write_csv', return_value=None):
        df_valid = validate_orders(spark, df_orders, params)

        assert df_valid is not None
        assert df_valid.count() == 3  # Only 3 valid unique records

        # Verify no invalid records in result
        assert df_valid.filter("oid IS NULL").count() == 0
        assert df_valid.filter("cid IS NULL").count() == 0
        assert df_valid.filter("oname IS NULL OR oname = ''").count() == 0

    print("✓ test_validate_orders passed")


def test_transform_derive_full_name():
    """Test full name derivation transformation"""
    sample_data = [
        (1, "john", "doe"),
        (2, "JANE", "SMITH"),
        (3, "  Bob  ", "  Johnson  ")
    ]

    df_customers = spark.createDataFrame(sample_data, ["cid", "fname", "lname"])
    df_result = transform_derive_full_name(df_customers)

    assert df_result is not None
    assert "full_name" in df_result.columns
    assert df_result.count() == 3

    # Verify title case formatting
    full_names = [row.full_name for row in df_result.collect()]
    assert "John Doe" in full_names
    assert "Jane Smith" in full_names
    assert "Bob Johnson" in full_names

    print("✓ test_transform_derive_full_name passed")


def test_transform_join_customer_orders():
    """Test customer-order join transformation"""
    customers_data = [
        (1, "John", "Doe", "John Doe"),
        (2, "Jane", "Smith", "Jane Smith"),
        (3, "Bob", "Johnson", "Bob Johnson")
    ]

    orders_data = [
        (101, 1, "Order_A"),
        (102, 1, "Order_B"),
        (103, 2, "Order_C"),
        (104, 4, "Order_D")  # No matching customer
    ]

    df_customers = spark.createDataFrame(
        customers_data,
        ["cid", "fname", "lname", "full_name"]
    )
    df_orders = spark.createDataFrame(orders_data, ["oid", "cid", "oname"])

    df_result = transform_join_customer_orders(df_customers, df_orders)

    assert df_result is not None
    assert df_result.count() == 3  # Only 3 matching records (inner join)
    assert "cid" in df_result.columns
    assert "full_name" in df_result.columns
    assert "oid" in df_result.columns
    assert "oname" in df_result.columns

    print("✓ test_transform_join_customer_orders passed")


def test_transform_calculate_order_count():
    """Test order count calculation transformation"""
    sample_data = [
        (1, "John", "Doe", "John Doe", 101, "Order_A"),
        (1, "John", "Doe", "John Doe", 102, "Order_B"),
        (2, "Jane", "Smith", "Jane Smith", 103, "Order_C"),
        (3, "Bob", "Johnson", "Bob Johnson", 104, "Order_D"),
        (3, "Bob", "Johnson", "Bob Johnson", 105, "Order_E"),
        (3, "Bob", "Johnson", "Bob Johnson", 106, "Order_F")
    ]

    df_joined = spark.createDataFrame(
        sample_data,
        ["cid", "fname", "lname", "full_name", "oid", "oname"]
    )

    df_result = transform_calculate_order_count(df_joined)

    assert df_result is not None
    assert "order_count" in df_result.columns

    # Verify order counts
    counts = df_result.select("cid", "order_count").distinct().collect()
    count_dict = {row.cid: row.order_count for row in counts}

    assert count_dict[1] == 2
    assert count_dict[2] == 1
    assert count_dict[3] == 3

    print("✓ test_transform_calculate_order_count passed")


def test_transform_add_processed_date():
    """Test processed date addition transformation"""
    sample_data = [
        (1, "John", "Doe", "John Doe", 101, "Order_A", 2),
        (2, "Jane", "Smith", "Jane Smith", 103, "Order_C", 1)
    ]

    df_input = spark.createDataFrame(
        sample_data,
        ["cid", "fname", "lname", "full_name", "oid", "oname", "order_count"]
    )

    df_result = transform_add_processed_date(df_input)

    assert df_result is not None
    assert "processed_date" in df_result.columns
    assert df_result.count() == 2

    # Verify processed_date is not null
    assert df_result.filter("processed_date IS NULL").count() == 0

    print("✓ test_transform_add_processed_date passed")


def test_load_customer_orders():
    """Test data loading to S3 with mocked write"""
    params = get_job_parameters()

    sample_data = [
        (1, "John", "Doe", "John Doe", 101, "Order_A", 2, datetime(2024, 1, 15).date()),
        (2, "Jane", "Smith", "Jane Smith", 103, "Order_C", 1, datetime(2024, 1, 15).date())
    ]

    df_final = spark.createDataFrame(
        sample_data,
        ["cid", "fname", "lname", "full_name", "oid", "oname", "order_count", "processed_date"]
    )

    # Mock the S3DataWriter._write_parquet method
    with patch.object(S3DataWriter, '_write_parquet', return_value=None):
        result = load_customer_orders(spark, df_final, params)

        assert result is True

    print("✓ test_load_customer_orders passed")


def test_s3_data_reader():
    """Test S3DataReader helper class"""
    reader = S3DataReader(spark)

    sample_data = [(1, "Test", "User")]
    mock_df = spark.createDataFrame(sample_data, ["CID", "FNAME", "LNAME"])

    # Mock the internal _read_csv method
    with patch.object(S3DataReader, '_read_csv', return_value=mock_df):
        df_result = reader.read_data_safe('s3://test/path', 'csv')

        assert df_result is not None
        # Verify column names are normalized to lowercase
        assert "cid" in df_result.columns
        assert "fname" in df_result.columns
        assert "lname" in df_result.columns

    print("✓ test_s3_data_reader passed")


def test_s3_data_writer():
    """Test S3DataWriter helper class"""
    writer = S3DataWriter(spark)

    sample_data = [(1, "Test", "User")]
    df_test = spark.createDataFrame(sample_data, ["cid", "fname", "lname"])

    # Mock the internal _write_parquet method
    with patch.object(S3DataWriter, '_write_parquet', return_value=None):
        result = writer.write_data_safe(df_test, 's3://test/path', 'parquet')
        assert result is True

    # Mock the internal _write_csv method
    with patch.object(S3DataWriter, '_write_csv', return_value=None):
        result = writer.write_data_safe(df_test, 's3://test/path', 'csv')
        assert result is True

    print("✓ test_s3_data_writer passed")


def test_end_to_end_pipeline():
    """Test complete ETL pipeline end-to-end"""
    params = get_job_parameters()

    # Create sample customer data
    customers_data = [
        (1, "john", "doe"),
        (2, "jane", "smith"),
        (3, "bob", "johnson")
    ]

    # Create sample order data
    orders_data = [
        (101, 1, "Order_A"),
        (102, 1, "Order_B"),
        (103, 2, "Order_C"),
        (104, 3, "Order_D"),
        (105, 3, "Order_E")
    ]

    mock_customers_df = spark.createDataFrame(customers_data, ["cid", "fname", "lname"])
    mock_orders_df = spark.createDataFrame(orders_data, ["oid", "cid", "oname"])

    # Mock all I/O operations
    with patch.object(S3DataReader, '_read_csv') as mock_read, \
         patch.object(S3DataWriter, '_write_csv') as mock_write_csv, \
         patch.object(S3DataWriter, '_write_parquet') as mock_write_parquet:

        # Setup mock to return appropriate dataframe based on path
        def read_side_effect(path, schema=None):
            if 'customers' in path:
                return mock_customers_df
            elif 'orders' in path:
                return mock_orders_df
            return None

        mock_read.side_effect = read_side_effect

        # Execute pipeline
        df_customers_raw = extract_customers(spark, params)
        df_orders_raw = extract_orders(spark, params)

        df_customers_valid = validate_customers(spark, df_customers_raw, params)
        df_orders_valid = validate_orders(spark, df_orders_raw, params)

        df_customers_transformed = transform_derive_full_name(df_customers_valid)
        df_joined = transform_join_customer_orders(df_customers_transformed, df_orders_valid)
        df_with_count = transform_calculate_order_count(df_joined)
        df_final = transform_add_processed_date(df_with_count)

        load_customer_orders(spark, df_final, params)

        # Verify final output
        assert df_final is not None
        assert df_final.count() == 5
        assert "full_name" in df_final.columns
        assert "order_count" in df_final.columns
        assert "processed_date" in df_final.columns

        # Verify order counts
        customer_1_count = df_final.filter("cid = 1").select("order_count").first()[0]
        customer_3_count = df_final.filter("cid = 3").select("order_count").first()[0]
        assert customer_1_count == 2
        assert customer_3_count == 2

    print("✓ test_end_to_end_pipeline passed")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Running Customer Order Analytics ETL Tests")
    print("=" * 80 + "\n")

    test_get_job_parameters()
    test_get_customer_schema()
    test_get_order_schema()
    test_extract_customers()
    test_extract_orders()
    test_validate_customers()
    test_validate_orders()
    test_transform_derive_full_name()
    test_transform_join_customer_orders()
    test_transform_calculate_order_count()
    test_transform_add_processed_date()
    test_load_customer_orders()
    test_s3_data_reader()
    test_s3_data_writer()
    test_end_to_end_pipeline()

    print("\n" + "=" * 80)
    print("All tests passed successfully! ✓")
    print("=" * 80 + "\n")

    spark.stop()