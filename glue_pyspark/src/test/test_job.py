"""
Comprehensive Test Suite for Customer Order ETL Job

Tests cover:
- FR-INGEST-001: Data ingestion from S3
- FR-CLEAN-001: Data quality validation
- FR-TRANSFORM-001: Business transformations
- FR-SCD2-001: SCD Type 2 implementation
- FR-CATALOG-001: Glue Catalog registration
- FR-MONITOR-001: Logging and monitoring

Framework: pytest
Mocking: pytest-mock, moto
Coverage Target: >80%

Author: Data Engineering Team
Version: 1.0.0
"""

import pytest
import yaml
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DecimalType, BooleanType, IntegerType
)
from pyspark.sql.functions import col

import boto3
from moto import mock_s3, mock_glue


# Test configuration
TEST_CONFIG = {
    'source_paths': {
        'customer_data': 's3://test-bucket/customers/',
        'order_data': 's3://test-bucket/orders/'
    },
    'target_paths': {
        'curated_customer': 's3://test-bucket/curated/customers/',
        'curated_order': 's3://test-bucket/curated/orders/',
        'customer_metrics': 's3://test-bucket/curated/metrics/',
        'quarantine_path': 's3://test-bucket/quarantine/'
    },
    'glue_catalog': {
        'database': 'test_db',
        'tables': {
            'customer_dimension': 'dim_customer',
            'order_fact': 'fact_order',
            'customer_metrics': 'agg_customer_metrics'
        }
    },
    'data_quality': {
        'customer': {
            'rules': [
                {
                    'name': 'email_format_validation',
                    'type': 'regex',
                    'column': 'email',
                    'pattern': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$',
                    'action': 'quarantine'
                },
                {
                    'name': 'valid_country_code',
                    'type': 'whitelist',
                    'column': 'country',
                    'values': ['US', 'CA', 'UK', 'DE', 'FR'],
                    'action': 'quarantine'
                },
                {
                    'name': 'registration_date_range',
                    'type': 'date_range',
                    'column': 'registration_date',
                    'min_date': '2020-01-01',
                    'max_date': 'current_date',
                    'action': 'quarantine'
                }
            ]
        },
        'order': {
            'rules': [
                {
                    'name': 'positive_amount',
                    'type': 'numeric_range',
                    'column': 'total_amount',
                    'min_value': 0.01,
                    'max_value': 1000000,
                    'action': 'quarantine'
                },
                {
                    'name': 'valid_order_status',
                    'type': 'whitelist',
                    'column': 'order_status',
                    'values': ['PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED'],
                    'action': 'quarantine'
                }
            ]
        }
    },
    'transformations': {
        'customer': {
            'rename_columns': {
                'customer_id': 'customer_key',
                'registration_date': 'registration_timestamp'
            },
            'cleansing': [
                {'column': 'email', 'operation': 'lowercase'},
                {'column': 'phone', 'operation': 'remove_special_chars', 'pattern': '[^0-9]'}
            ],
            'derived_columns': [
                {'name': 'full_name', 'expression': "concat(first_name, ' ', last_name)"},
                {'name': 'registration_year', 'expression': 'year(registration_timestamp)'},
                {'name': 'customer_age_days', 'expression': 'datediff(current_date(), registration_timestamp)'}
            ]
        },
        'order': {
            'rename_columns': {
                'order_id': 'order_key',
                'customer_id': 'customer_key'
            },
            'derived_columns': [
                {'name': 'order_year', 'expression': 'year(order_date)'},
                {'name': 'order_month', 'expression': 'month(order_date)'},
                {'name': 'days_since_order', 'expression': 'datediff(current_date(), order_date)'}
            ],
            'aggregations': {
                'group_by': ['customer_key'],
                'metrics': [
                    {'name': 'total_orders', 'expression': 'count(*)'},
                    {'name': 'total_revenue', 'expression': 'sum(total_amount)'},
                    {'name': 'avg_order_value', 'expression': 'avg(total_amount)'}
                ]
            }
        }
    },
    'scd_type2': {
        'customer': {
            'enabled': True,
            'record_key': 'customer_key',
            'precombine_field': 'updated_at',
            'partition_path': 'country,registration_year',
            'hudi_options': {
                'table_name': 'customer_dimension',
                'table_type': 'COPY_ON_WRITE',
                'operation': 'upsert',
                'upsert_parallelism': '100',
                'bulk_insert_parallelism': '100',
                'delete_parallelism': '100',
                'inline_compact': 'true',
                'inline_compact_max_delta_commits': '5',
                'cleaner_policy': 'KEEP_LATEST_COMMITS',
                'cleaner_commits_retained': '10',
                'index_type': 'BLOOM'
            }
        },
        'order': {
            'enabled': True,
            'record_key': 'order_key',
            'precombine_field': 'updated_at',
            'partition_path': 'order_year,order_month',
            'hudi_options': {
                'table_name': 'order_fact',
                'table_type': 'COPY_ON_WRITE',
                'operation': 'upsert',
                'upsert_parallelism': '200',
                'bulk_insert_parallelism': '200'
            }
        }
    },
    'spark_config': {
        'default_parallelism': '200',
        'sql_shuffle_partitions': '200'
    }
}


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_customer_order_etl") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def sample_customer_data(spark_session):
    """Generate sample customer data for testing"""
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), False),
        StructField("postal_code", StringType(), True),
        StructField("registration_date", TimestampType(), False),
        StructField("customer_status", StringType(), False)
    ])

    data = [
        ("C001", "John", "Doe", "john.doe@email.com", "1234567890",
         "123 Main St", "New York", "NY", "US", "10001",
         datetime(2023, 1, 15), "ACTIVE"),
        ("C002", "Jane", "Smith", "jane.smith@email.com", "9876543210",
         "456 Oak Ave", "Toronto", "ON", "CA", "M5H2N2",
         datetime(2023, 2, 20), "ACTIVE"),
        ("C003", "Bob", "Johnson", "INVALID_EMAIL", "5555555555",
         "789 Pine Rd", "London", "LDN", "UK", "SW1A1AA",
         datetime(2023, 3, 10), "ACTIVE"),
        ("C004", "Alice", "Williams", "alice.w@email.com", None,
         "321 Elm St", "Berlin", "BE", "DE", "10115",
         datetime(2023, 4, 5), "INACTIVE"),
        ("C005", "Charlie", "Brown", "charlie@email.com", "1112223333",
         "654 Maple Dr", "Paris", "IDF", "FR", "75001",
         datetime(2023, 5, 12), "ACTIVE")
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark_session):
    """Generate sample order data for testing"""
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("order_status", StringType(), False),
        StructField("total_amount", DecimalType(10, 2), False),
        StructField("currency", StringType(), False),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), True)
    ])

    data = [
        ("O001", "C001", datetime(2023, 6, 1), "DELIVERED",
         Decimal("150.50"), "USD", "CREDIT_CARD", "123 Main St",
         datetime(2023, 6, 1), datetime(2023, 6, 1)),
        ("O002", "C001", datetime(2023, 6, 15), "SHIPPED",
         Decimal("275.00"), "USD", "PAYPAL", "123 Main St",
         datetime(2023, 6, 15), datetime(2023, 6, 15)),
        ("O003", "C002", datetime(2023, 6, 20), "CONFIRMED",
         Decimal("89.99"), "CAD", "CREDIT_CARD", "456 Oak Ave",
         datetime(2023, 6, 20), datetime(2023, 6, 20)),
        ("O004", "C003", datetime(2023, 7, 1), "PENDING",
         Decimal("-50.00"), "GBP", "DEBIT_CARD", "789 Pine Rd",
         datetime(2023, 7, 1), datetime(2023, 7, 1)),
        ("O005", "C005", datetime(2023, 7, 10), "DELIVERED",
         Decimal("320.75"), "EUR", "CREDIT_CARD", "654 Maple Dr",
         datetime(2023, 7, 10), datetime(2023, 7, 10))
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def mock_config(tmp_path):
    """Create mock configuration file"""
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(TEST_CONFIG, f)
    return str(config_file)


@pytest.fixture
def mock_s3_setup():
    """Setup mock S3 environment"""
    with mock_s3():
        s3_client = boto3.client('s3', region_name='us-east-1')

        # Create test buckets
        s3_client.create_bucket(Bucket='test-bucket')

        yield s3_client


@pytest.fixture
def mock_glue_setup():
    """Setup mock Glue environment"""
    with mock_glue():
        glue_client = boto3.client('glue', region_name='us-east-1')

        # Create test database
        glue_client.create_database(
            DatabaseInput={
                'Name': 'test_db',
                'Description': 'Test database'
            }
        )

        yield glue_client


# ============================================================================
# UNIT TESTS
# ============================================================================

@pytest.mark.unit
class TestS3PathValidator:
    """Test S3 path validation functionality"""

    def test_parse_s3_path_valid(self):
        """Test parsing valid S3 paths"""
        from src.main.job import S3PathValidator

        validator = S3PathValidator()
        bucket, key = validator._parse_s3_path('s3://my-bucket/my/key/path/')

        assert bucket == 'my-bucket'
        assert key == 'my/key/path/'

    def test_parse_s3_path_invalid(self):
        """Test parsing invalid S3 paths"""
        from src.main.job import S3PathValidator

        validator = S3PathValidator()

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            validator._parse_s3_path('invalid-path')

    def test_validate_read_path_success(self, mock_s3_setup):
        """Test successful read path validation"""
        from src.main.job import S3PathValidator

        # Upload test file
        mock_s3_setup.put_object(
            Bucket='test-bucket',
            Key='customers/data.csv',
            Body=b'test data'
        )

        validator = S3PathValidator()
        result = validator.validate_read_path('s3://test-bucket/customers/')

        assert result is True

    def test_validate_read_path_no_objects(self, mock_s3_setup):
        """Test read path validation with no objects"""
        from src.main.job import S3PathValidator

        validator = S3PathValidator()
        result = validator.validate_read_path('s3://test-bucket/empty/')

        assert result is False

    def test_validate_write_path_success(self, mock_s3_setup):
        """Test successful write path validation"""
        from src.main.job import S3PathValidator

        validator = S3PathValidator()
        result = validator.validate_write_path('s3://test-bucket/output/')

        assert result is True


@pytest.mark.unit
class TestDataQualityValidator:
    """Test data quality validation rules"""

    def test_email_regex_validation(self, spark_session, sample_customer_data):
        """Test email format validation rule - FR-CLEAN-001"""
        from src.main.job import DataQualityValidator

        validator = DataQualityValidator(spark_session, TEST_CONFIG)
        valid_df, invalid_df = validator.validate_customer_data(sample_customer_data)

        # Check that invalid email is caught
        invalid_emails = invalid_df.select('email').collect()
        assert any('INVALID_EMAIL' in row.email for row in invalid_emails)

    def test_country_whitelist_validation(self, spark_session):
        """Test country code whitelist validation - FR-CLEAN-001"""
        from src.main.job import DataQualityValidator

        # Create data with invalid country
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), False),
            StructField("country", StringType(), False),
            StructField("registration_date", TimestampType(), False)
        ])

        data = [
            ("C001", "test@email.com", "XX", datetime(2023, 1, 1))
        ]

        df = spark_session.createDataFrame(data, schema)

        validator = DataQualityValidator(spark_session, TEST_CONFIG)
        valid_df, invalid_df = validator.validate_customer_data(df)

        assert invalid_df.count() == 1

    def test_numeric_range_validation(self, spark_session, sample_order_data):
        """Test numeric range validation for order amounts - FR-CLEAN-001"""
        from src.main.job import DataQualityValidator

        validator = DataQualityValidator(spark_session, TEST_CONFIG)
        valid_df, invalid_df = validator.validate_order_data(sample_order_data)

        # Check that negative amount is caught
        invalid_amounts = invalid_df.select('total_amount').collect()
        assert any(row.total_amount < 0 for row in invalid_amounts)

    def test_order_status_whitelist(self, spark_session):
        """Test order status whitelist validation - FR-CLEAN-001"""
        from src.main.job import DataQualityValidator

        # Create data with invalid status
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("order_status", StringType(), False),
            StructField("total_amount", DecimalType(10, 2), False),
            StructField("order_date", TimestampType(), False)
        ])

        data = [
            ("O001", "INVALID_STATUS", Decimal("100.00"), datetime(2023, 1, 1))
        ]

        df = spark_session.createDataFrame(data, schema)

        validator = DataQualityValidator(spark_session, TEST_CONFIG)
        valid_df, invalid_df = validator.validate_order_data(df)

        assert invalid_df.count() == 1


@pytest.mark.unit
class TestTransformations:
    """Test data transformation logic"""

    def test_customer_column_renaming(self, spark_session, sample_customer_data):
        """Test customer column renaming - FR-TRANSFORM-001"""
        from src.main.job import CustomerOrderETL

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            transformed_df = etl.transform_customer_data(sample_customer_data)

            # Check renamed columns exist
            assert 'customer_key' in transformed_df.columns
            assert 'registration_timestamp' in transformed_df.columns
            assert 'customer_id' not in transformed_df.columns

    def test_customer_derived_columns(self, spark_session, sample_customer_data):
        """Test customer derived column creation - FR-TRANSFORM-001"""
        from src.main.job import CustomerOrderETL

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            transformed_df = etl.transform_customer_data(sample_customer_data)

            # Check derived columns exist
            assert 'full_name' in transformed_df.columns
            assert 'registration_year' in transformed_df.columns
            assert 'customer_age_days' in transformed_df.columns

            # Verify full_name concatenation
            first_row = transformed_df.first()
            assert first_row.full_name == f"{first_row.first_name} {first_row.last_name}"

    def test_customer_scd2_columns(self, spark_session, sample_customer_data):
        """Test SCD Type 2 column addition - FR-SCD2-001"""
        from src.main.job import CustomerOrderETL

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            transformed_df = etl.transform_customer_data(sample_customer_data)

            # Check SCD Type 2 columns
            assert 'is_active' in transformed_df.columns
            assert 'effective_start_date' in transformed_df.columns
            assert 'effective_end_date' in transformed_df.columns
            assert 'op_ts' in transformed_df.columns

            # Verify is_active is True for all new records
            assert all(row.is_active for row in transformed_df.collect())

    def test_order_derived_columns(self, spark_session, sample_order_data):
        """Test order derived column creation - FR-TRANSFORM-001"""
        from src.main.job import CustomerOrderETL

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            transformed_df = etl.transform_order_data(sample_order_data)

            # Check derived columns
            assert 'order_year' in transformed_df.columns
            assert 'order_month' in transformed_df.columns
            assert 'days_since_order' in transformed_df.columns

    def test_email_lowercase_cleansing(self, spark_session):
        """Test email lowercase cleansing - FR-CLEAN-001"""
        from src.main.job import CustomerOrderETL

        # Create data with uppercase email
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), False),
            StructField("registration_date", TimestampType(), False)
        ])

        data = [("C001", "TEST@EMAIL.COM", datetime(2023, 1, 1))]
        df = spark_session.createDataFrame(data, schema)

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            transformed_df = etl.transform_customer_data(df)

            # Verify email is lowercase
            assert transformed_df.first().email == "test@email.com"


@pytest.mark.unit
class TestCustomerMetrics:
    """Test customer metrics calculation"""

    def test_calculate_customer_metrics(self, spark_session, sample_order_data):
        """Test customer metrics aggregation - FR-TRANSFORM-001"""
        from src.main.job import CustomerOrderETL

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            # First transform order data to get customer_key
            transformed_orders = etl.transform_order_data(sample_order_data)

            # Calculate metrics
            metrics_df = etl.calculate_customer_metrics(transformed_orders)

            # Verify metrics columns
            assert 'total_orders' in metrics_df.columns
            assert 'total_revenue' in metrics_df.columns
            assert 'avg_order_value' in metrics_df.columns
            assert 'first_order_date' in metrics_df.columns
            assert 'last_order_date' in metrics_df.columns
            assert 'customer_lifetime_days' in metrics_df.columns

    def test_customer_metrics_aggregation_accuracy(self, spark_session):
        """Test accuracy of customer metrics calculations"""
        from src.main.job import CustomerOrderETL

        # Create specific test data
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_date", TimestampType(), False),
            StructField("total_amount", DecimalType(10, 2), False),
            StructField("created_at", TimestampType(), False)
        ])

        data = [
            ("O001", "C001", datetime(2023, 1, 1), Decimal("100.00"), datetime(2023, 1, 1)),
            ("O002", "C001", datetime(2023, 1, 15), Decimal("200.00"), datetime(2023, 1, 15)),
            ("O003", "C001", datetime(2023, 2, 1), Decimal("150.00"), datetime(2023, 2, 1))
        ]

        df = spark_session.createDataFrame(data, schema)

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            transformed_df = etl.transform_order_data(df)
            metrics_df = etl.calculate_customer_metrics(transformed_df)

            # Get metrics for C001
            c001_metrics = metrics_df.filter(col('customer_key') == 'C001').first()

            assert c001_metrics.total_orders == 3
            assert float(c001_metrics.total_revenue) == 450.00
            assert float(c001_metrics.avg_order_value) == 150.00


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestEndToEndDataFlow:
    """Test complete end-to-end data flow"""

    @patch('src.main.job.CustomerOrderETL.initialize_spark_contexts')
    @patch('src.main.job.S3PathValidator.validate_read_path')
    @patch('src.main.job.S3PathValidator.validate_write_path')
    def test_customer_data_pipeline(
        self,
        mock_write_path,
        mock_read_path,
        mock_init_contexts,
        spark_session,
        sample_customer_data,
        mock_config
    ):
        """Test complete customer data pipeline - FR-INGEST-001, FR-CLEAN-001, FR-TRANSFORM-001"""
        from src.main.job import CustomerOrderETL, DataQualityValidator

        # Setup mocks
        mock_read_path.return_value = True
        mock_write_path.return_value = True

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL(mock_config)
            etl.spark = spark_session
            etl.s3_validator = Mock()
            etl.s3_validator.validate_read_path.return_value = True
            etl.s3_validator.validate_write_path.return_value = True

            # Run pipeline steps
            dq_validator = DataQualityValidator(spark_session, TEST_CONFIG)

            # Validate
            valid_df, invalid_df = dq_validator.validate_customer_data(sample_customer_data)

            # Transform
            transformed_df = etl.transform_customer_data(valid_df)

            # Verify results
            assert transformed_df.count() > 0
            assert 'customer_key' in transformed_df.columns
            assert 'is_active' in transformed_df.columns
            assert 'effective_start_date' in transformed_df.columns

    @patch('src.main.job.CustomerOrderETL.initialize_spark_contexts')
    @patch('src.main.job.S3PathValidator.validate_read_path')
    @patch('src.main.job.S3PathValidator.validate_write_path')
    def test_order_data_pipeline(
        self,
        mock_write_path,
        mock_read_path,
        mock_init_contexts,
        spark_session,
        sample_order_data,
        mock_config
    ):
        """Test complete order data pipeline - FR-INGEST-001, FR-CLEAN-001, FR-TRANSFORM-001"""
        from src.main.job import CustomerOrderETL, DataQualityValidator

        # Setup mocks
        mock_read_path.return_value = True
        mock_write_path.return_value = True

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL(mock_config)
            etl.spark = spark_session
            etl.s3_validator = Mock()
            etl.s3_validator.validate_read_path.return_value = True
            etl.s3_validator.validate_write_path.return_value = True

            # Run pipeline steps
            dq_validator = DataQualityValidator(spark_session, TEST_CONFIG)

            # Validate
            valid_df, invalid_df = dq_validator.validate_order_data(sample_order_data)

            # Transform
            transformed_df = etl.transform_order_data(valid_df)

            # Calculate metrics
            metrics_df = etl.calculate_customer_metrics(transformed_df)

            # Verify results
            assert transformed_df.count() > 0
            assert metrics_df.count() > 0
            assert 'order_key' in transformed_df.columns
            assert 'total_orders' in metrics_df.columns


@pytest.mark.integration
class TestHudiIntegration:
    """Test Hudi SCD Type 2 integration"""

    @patch('src.main.job.CustomerOrderETL._register_glue_table')
    def test_hudi_write_options_customer(
        self,
        mock_register_table,
        spark_session,
        sample_customer_data,
        tmp_path
    ):
        """Test Hudi write options for customer dimension - FR-SCD2-001"""
        from src.main.job import CustomerOrderETL

        # Update config with temp path
        test_config = TEST_CONFIG.copy()
        test_config['target_paths']['curated_customer'] = str(tmp_path / 'customer')

        with patch.object(CustomerOrderETL, '_load_config', return_value=test_config):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session
            etl.s3_validator = Mock()
            etl.s3_validator.validate_write_path.return_value = True

            # Transform data
            transformed_df = etl.transform_customer_data(sample_customer_data)

            # Mock the write operation to capture options
            with patch.object(transformed_df.write, 'save') as mock_save:
                with patch.object(transformed_df.write, 'format') as mock_format:
                    with patch.object(transformed_df.write, 'options') as mock_options:
                        with patch.object(transformed_df.write, 'mode') as mock_mode:
                            mock_format.return_value = transformed_df.write
                            mock_options.return_value = transformed_df.write
                            mock_mode.return_value = transformed_df.write

                            # Call write method
                            etl.write_customer_hudi(transformed_df)

                            # Verify Hudi format was used
                            mock_format.assert_called_once_with('hudi')

    def test_scd2_column_presence(self, spark_session, sample_customer_data):
        """Test SCD Type 2 columns are present - FR-SCD2-001"""
        from src.main.job import CustomerOrderETL

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            transformed_df = etl.transform_customer_data(sample_customer_data)

            # Verify all SCD Type 2 columns
            required_columns = ['is_active', 'effective_start_date', 'effective_end_date', 'op_ts']
            for col_name in required_columns:
                assert col_name in transformed_df.columns, f"Missing SCD Type 2 column: {col_name}"


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

@pytest.mark.unit
class TestErrorHandling:
    """Test error handling and edge cases"""

    def test_invalid_s3_path_format(self):
        """Test handling of invalid S3 path format"""
        from src.main.job import S3PathValidator

        validator = S3PathValidator()

        with pytest.raises(ValueError, match="Invalid S3 path format"):
            validator._parse_s3_path('not-an-s3-path')

    def test_missing_configuration_file(self):
        """Test handling of missing configuration file"""
        from src.main.job import CustomerOrderETL

        with pytest.raises(Exception):
            etl = CustomerOrderETL('/nonexistent/config.yaml')

    def test_empty_dataframe_handling(self, spark_session):
        """Test handling of empty DataFrames"""
        from src.main.job import CustomerOrderETL

        # Create empty DataFrame
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), False)
        ])

        empty_df = spark_session.createDataFrame([], schema)

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            # Should handle empty DataFrame gracefully
            transformed_df = etl.transform_customer_data(empty_df)
            assert transformed_df.count() == 0

    @patch('src.main.job.boto3.client')
    def test_s3_access_denied(self, mock_boto_client):
        """Test handling of S3 access denied errors"""
        from src.main.job import S3PathValidator
        from botocore.exceptions import ClientError

        # Mock S3 client to raise access denied
        mock_s3 = Mock()
        mock_s3.head_bucket.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied'}},
            'HeadBucket'
        )
        mock_boto_client.return_value = mock_s3

        validator = S3PathValidator()
        result = validator.validate_read_path('s3://test-bucket/path/')

        assert result is False

    def test_null_value_handling(self, spark_session):
        """Test handling of null values in data"""
        from src.main.job import CustomerOrderETL

        # Create data with nulls
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("registration_date", TimestampType(), False)
        ])

        data = [
            ("C001", None, None, datetime(2023, 1, 1))
        ]

        df = spark_session.createDataFrame(data, schema)

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            # Should handle nulls without crashing
            transformed_df = etl.transform_customer_data(df)
            assert transformed_df.count() == 1


# ============================================================================
# DATA QUALITY TESTS
# ============================================================================

@pytest.mark.data_quality
class TestDataQualityRules:
    """Test specific data quality rules"""

    def test_quarantine_invalid_records(self, spark_session, sample_customer_data, tmp_path):
        """Test that invalid records are quarantined - FR-CLEAN-001"""
        from src.main.job import DataQualityValidator

        # Update config with temp quarantine path
        test_config = TEST_CONFIG.copy()
        test_config['target_paths']['quarantine_path'] = str(tmp_path / 'quarantine/')

        validator = DataQualityValidator(spark_session, test_config)
        valid_df, invalid_df = validator.validate_customer_data(sample_customer_data)

        # Should have some invalid records
        assert invalid_df.count() > 0

        # Invalid records should have validation errors
        assert '_validation_errors' in invalid_df.columns

    def test_data_quality_pass_rate(self, spark_session, sample_customer_data):
        """Test calculation of data quality pass rate"""
        from src.main.job import DataQualityValidator

        validator = DataQualityValidator(spark_session, TEST_CONFIG)
        valid_df, invalid_df = validator.validate_customer_data(sample_customer_data)

        total_records = sample_customer_data.count()
        valid_records = valid_df.count()
        pass_rate = (valid_records / total_records) * 100

        # Pass rate should be reasonable (not 0% or 100% with our test data)
        assert 0 < pass_rate < 100


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

@pytest.mark.slow
class TestPerformance:
    """Test performance characteristics"""

    def test_large_dataset_processing(self, spark_session):
        """Test processing of larger datasets"""
        from src.main.job import CustomerOrderETL

        # Generate larger dataset
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), False),
            StructField("country", StringType(), False),
            StructField("registration_date", TimestampType(), False)
        ])

        # Create 10000 records
        data = [
            (f"C{i:05d}", f"customer{i}@email.com", "US", datetime(2023, 1, 1))
            for i in range(10000)
        ]

        large_df = spark_session.createDataFrame(data, schema)

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            import time
            start_time = time.time()

            transformed_df = etl.transform_customer_data(large_df)
            result_count = transformed_df.count()

            end_time = time.time()
            duration = end_time - start_time

            # Should process 10000 records in reasonable time (< 30 seconds)
            assert duration < 30
            assert result_count == 10000


# ============================================================================
# GLUE CATALOG TESTS
# ============================================================================

@pytest.mark.aws
class TestGlueCatalogIntegration:
    """Test Glue Catalog integration"""

    @patch('boto3.client')
    def test_register_glue_table(self, mock_boto_client, spark_session):
        """Test Glue Catalog table registration - FR-CATALOG-001"""
        from src.main.job import CustomerOrderETL

        # Mock Glue client
        mock_glue = Mock()
        mock_glue.get_database.return_value = {'Database': {'Name': 'test_db'}}
        mock_boto_client.return_value = mock_glue

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            # Call register method
            etl._register_glue_table(
                'customer',
                's3://test-bucket/customer/',
                'dim_customer'
            )

            # Verify Glue client was called
            mock_glue.get_database.assert_called_once_with(Name='test_db')

    @patch('boto3.client')
    def test_create_database_if_not_exists(self, mock_boto_client, spark_session):
        """Test Glue database creation if not exists"""
        from src.main.job import CustomerOrderETL
        from botocore.exceptions import ClientError

        # Mock Glue client to simulate database not found
        mock_glue = Mock()
        mock_glue.exceptions.EntityNotFoundException = type('EntityNotFoundException', (Exception,), {})
        mock_glue.get_database.side_effect = mock_glue.exceptions.EntityNotFoundException()
        mock_boto_client.return_value = mock_glue

        with patch.object(CustomerOrderETL, '_load_config', return_value=TEST_CONFIG):
            etl = CustomerOrderETL('dummy_path')
            etl.spark = spark_session

            # Call register method
            etl._register_glue_table(
                'customer',
                's3://test-bucket/customer/',
                'dim_customer'
            )

            # Verify database creation was attempted
            mock_glue.create_database.assert_called_once()


# ============================================================================
# CONFIGURATION TESTS
# ============================================================================

@pytest.mark.unit
class TestConfiguration:
    """Test configuration loading and validation"""

    def test_load_config_from_file(self, mock_config):
        """Test loading configuration from local file"""
        from src.main.job import CustomerOrderETL

        etl = CustomerOrderETL(mock_config)

        assert etl.config is not None
        assert 'source_paths' in etl.config
        assert 'target_paths' in etl.config

    @patch('boto3.client')
    def test_load_config_from_s3(self, mock_boto_client):
        """Test loading configuration from S3"""
        from src.main.job import CustomerOrderETL

        # Mock S3 client
        mock_s3 = Mock()
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = yaml.dump(TEST_CONFIG).encode('utf-8')
        mock_s3.get_object.return_value = mock_response
        mock_boto_client.return_value = mock_s3

        etl = CustomerOrderETL('s3://config-bucket/config.yaml')

        assert etl.config is not None
        mock_s3.get_object.assert_called_once()

    def test_config_has_required_sections(self, mock_config):
        """Test configuration has all required sections"""
        from src.main.job import CustomerOrderETL

        etl = CustomerOrderETL(mock_config)

        required_sections = [
            'source_paths',
            'target_paths',
            'glue_catalog',
            'data_quality',
            'transformations',
            'scd_type2'
        ]

        for section in required_sections:
            assert section in etl.config, f"Missing required config section: {section}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=src/main", "--cov-report=html"])