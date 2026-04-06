"""
Pytest fixtures for AWS Glue ETL Job testing
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType
)
from unittest.mock import Mock, MagicMock, patch
import yaml
import os


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_glue_job") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def mock_glue_context():
    """Mock AWS Glue Context"""
    mock_context = Mock()
    mock_context.spark_session = None  # Will be set by test
    mock_context.create_database = Mock()
    return mock_context


@pytest.fixture
def sample_params():
    """Sample job parameters matching YAML structure"""
    return {
        'inputs_customer_raw_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
        'inputs_customer_curated_path': 's3://adif-sdlc/curated/sdlc_wizard/customer/',
        'inputs_order_raw_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
        'inputs_order_curated_path': 's3://adif-sdlc/curated/sdlc_wizard/order/',
        'inputs_ordersummary_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
        'inputs_customer_format': 'csv',
        'inputs_order_format': 'csv',
        'inputs_delimiter': ',',
        'outputs_customer_aggregate_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
        'outputs_customer_scd2_path': 's3://adif-sdlc/curated/sdlc_wizard/customer_scd2/',
        'outputs_format': 'hudi',
        'glue_database_name': 'sdlc_wizard_db',
        'glue_customer_table_name': 'customer_scd2',
        'glue_aggregate_table_name': 'customer_aggregate_spend',
        'hudi_customer_table_name': 'customer_scd2',
        'hudi_customer_record_key': 'CustId',
        'hudi_customer_precombine_field': 'OpTs',
        'hudi_customer_partition_field': 'Region',
        'hudi_operation_type': 'upsert',
        'hudi_table_type': 'COPY_ON_WRITE',
        'hudi_aggregate_table_name': 'customer_aggregate_spend',
        'hudi_aggregate_record_key': 'CustId',
        'hudi_aggregate_precombine_field': 'OpTs',
        'hudi_aggregate_partition_field': 'Region',
        'customer_schema': [
            {'name': 'CustId', 'type': 'string'},
            {'name': 'Name', 'type': 'string'},
            {'name': 'EmailId', 'type': 'string'},
            {'name': 'Region', 'type': 'string'}
        ],
        'order_schema': [
            {'name': 'OrderId', 'type': 'string'},
            {'name': 'CustId', 'type': 'string'},
            {'name': 'OrderAmount', 'type': 'double'},
            {'name': 'OrderDate', 'type': 'timestamp'}
        ],
        'scd2_columns': {
            'is_active': 'IsActive',
            'start_date': 'StartDate',
            'end_date': 'EndDate',
            'operation_timestamp': 'OpTs'
        },
        'data_quality': {
            'remove_nulls': True,
            'remove_null_strings': True,
            'remove_duplicates': True,
            'null_string_values': ['Null', 'NULL', 'null']
        },
        'aggregation': {
            'group_by_columns': ['CustId', 'Region'],
            'aggregate_columns': {
                'total_spend': 'sum(OrderAmount)',
                'order_count': 'count(OrderId)',
                'avg_order_value': 'avg(OrderAmount)',
                'first_order_date': 'min(OrderDate)',
                'last_order_date': 'max(OrderDate)'
            }
        },
        'job_name': 'customer-order-analytics-scd2',
        'job_bookmark_enabled': True,
        'enable_glue_datacatalog': True
    }


@pytest.fixture
def customer_schema():
    """Customer data schema"""
    return StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])


@pytest.fixture
def order_schema():
    """Order data schema"""
    return StructType([
        StructField("OrderId", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("OrderAmount", DoubleType(), True),
        StructField("OrderDate", TimestampType(), True)
    ])


@pytest.fixture
def scd2_schema():
    """SCD Type 2 schema with tracking columns"""
    return StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("IsActive", BooleanType(), True),
        StructField("StartDate", TimestampType(), True),
        StructField("EndDate", TimestampType(), True),
        StructField("OpTs", TimestampType(), True)
    ])


@pytest.fixture
def sample_customer_data(spark, customer_schema):
    """Sample customer data for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West")
    ]
    return spark.createDataFrame(data, customer_schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark, customer_schema):
    """Sample customer data with NULL and 'Null' values"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        ("C004", "Alice Brown", "alice@example.com", None),
        ("C005", "Null", "charlie@example.com", "West")
    ]
    return spark.createDataFrame(data, customer_schema)


@pytest.fixture
def sample_customer_data_with_duplicates(spark, customer_schema):
    """Sample customer data with duplicates"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East")
    ]
    return spark.createDataFrame(data, customer_schema)


@pytest.fixture
def sample_order_data(spark, order_schema):
    """Sample order data for testing"""
    data = [
        ("O001", "C001", 100.50, datetime(2024, 1, 1)),
        ("O002", "C001", 200.75, datetime(2024, 1, 5)),
        ("O003", "C002", 150.00, datetime(2024, 1, 3)),
        ("O004", "C003", 300.25, datetime(2024, 1, 7)),
        ("O005", "C001", 50.00, datetime(2024, 1, 10))
    ]
    return spark.createDataFrame(data, order_schema)


@pytest.fixture
def sample_scd2_existing_data(spark, scd2_schema):
    """Sample existing SCD2 data"""
    data = [
        ("C001", "John Doe", "john@example.com", "North",
         True, datetime(2024, 1, 1), None, datetime(2024, 1, 1)),
        ("C002", "Jane Smith", "jane@example.com", "South",
         True, datetime(2024, 1, 1), None, datetime(2024, 1, 1))
    ]
    return spark.createDataFrame(data, scd2_schema)


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client"""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        yield mock_s3


@pytest.fixture
def mock_glue_client():
    """Mock boto3 Glue client"""
    with patch('boto3.client') as mock_client:
        mock_glue = Mock()
        mock_client.return_value = mock_glue
        yield mock_glue