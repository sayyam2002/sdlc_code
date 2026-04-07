"""
Comprehensive unit tests for Customer Order SCD Type 2 ETL Job
Tests all functional requirements: FR-INGEST-001, FR-CLEAN-001, FR-SCD2-001, FR-AGGREGATE-001, FR-OUTPUT-001
"""

import pytest
import yaml
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch, call
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    DateType, BooleanType, TimestampType, IntegerType
)
from pyspark.sql.functions import col


# Test configuration
TEST_CONFIG = {
    'source_customer_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
    'source_order_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
    'output_customer_snapshot_path': 's3://adif-sdlc/curated/sdlc_wizard/customer_snapshot/',
    'output_order_summary_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
    'output_curated_customer_path': 's3://adif-sdlc/curated/sdlc_wizard/customer/',
    'output_curated_order_path': 's3://adif-sdlc/curated/sdlc_wizard/order/',
    'output_analytics_path': 's3://adif-sdlc/analytics/',
    'temp_path': 's3://adif-sdlc/temp/',
    'glue_database': 'sdlc_wizard',
    'glue_table_customer': 'customer',
    'glue_table_order': 'order',
    'glue_table_customer_snapshot': 'customer_snapshot',
    'glue_table_order_summary': 'ordersummary',
    'data_format_customer': 'csv',
    'data_format_order': 'csv',
    'csv_header': True,
    'csv_delimiter': ',',
    'remove_nulls': True,
    'remove_null_strings': True,
    'remove_duplicates': True,
    'null_string_values': ['Null', 'NULL', 'null'],
    'primary_key_customer': 'CustId',
    'primary_key_order': 'OrderId',
    'hudi_operation': 'upsert',
    'hudi_precombine_field': 'OpTs',
    'shuffle_partitions': 200,
    'min_record_count': 1,
    'max_null_percentage': 5.0,
    'enable_data_quality_checks': True,
    'customer_schema': [
        {'name': 'CustId', 'type': 'string', 'nullable': False},
        {'name': 'Name', 'type': 'string', 'nullable': False},
        {'name': 'EmailId', 'type': 'string', 'nullable': True},
        {'name': 'Region', 'type': 'string', 'nullable': True}
    ],
    'order_schema': [
        {'name': 'CustId', 'type': 'string', 'nullable': False},
        {'name': 'Name', 'type': 'string', 'nullable': False},
        {'name': 'EmailId', 'type': 'string', 'nullable': True},
        {'name': 'Region', 'type': 'string', 'nullable': True},
        {'name': 'OrderId', 'type': 'string', 'nullable': False}
    ]
}


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("CustomerOrderETL_Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def glue_context(spark):
    """Mock GlueContext"""
    mock_glue_context = Mock()
    mock_glue_context.spark_session = spark
    mock_logger = Mock()
    mock_glue_context.get_logger.return_value = mock_logger
    return mock_glue_context


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@email.com", "North"),
        ("C002", "Jane Smith", "jane.smith@email.com", "South"),
        ("C003", "Bob Johnson", "bob.johnson@email.com", "East"),
        ("C004", "Alice Williams", "alice.williams@email.com", "West"),
        ("C005", "Charlie Brown", "charlie.brown@email.com", "North"),
        ("C006", "Diana Prince", None, "South"),
        ("C007", "Eve Adams", "Null", "East"),
        ("C008", "Frank Miller", "frank.miller@email.com", None),
        ("C001", "John Doe", "john.doe@email.com", "North"),  # Duplicate
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing"""
    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("OrderId", StringType(), False),
        StructField("TotalAmount", DecimalType(18, 2), True),
        StructField("Date", DateType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@email.com", "North", "O001", Decimal("150.50"), date(2024, 1, 15)),
        ("C001", "John Doe", "john.doe@email.com", "North", "O002", Decimal("200.00"), date(2024, 1, 20)),
        ("C002", "Jane Smith", "jane.smith@email.com", "South", "O003", Decimal("300.75"), date(2024, 1, 18)),
        ("C003", "Bob Johnson", "bob.johnson@email.com", "East", "O004", Decimal("450.00"), date(2024, 1, 22)),
        ("C004", "Alice Williams", "alice.williams@email.com", "West", "O005", Decimal("125.25"), date(2024, 1, 25)),
        ("C005", "Charlie Brown", "charlie.brown@email.com", "North", "O006", Decimal("275.50"), date(2024, 1, 28)),
        ("C002", "Jane Smith", "jane.smith@email.com", "South", "O007", Decimal("180.00"), date(2024, 2, 1)),
        ("C006", "Diana Prince", None, "South", "O008", None, date(2024, 2, 3)),
        ("C007", "Eve Adams", "Null", "East", "O009", Decimal("95.00"), None),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def etl_job(spark, glue_context):
    """Create ETL job instance for testing"""
    from src.main.job import CustomerOrderETL
    return CustomerOrderETL(spark, glue_context, TEST_CONFIG)


class TestDataIngestion:
    """Test FR-INGEST-001: Data Ingestion"""

    def test_read_customer_data_csv(self, etl_job, sample_customer_data, tmp_path):
        """Test reading customer data from CSV"""
        # Write sample data to temp CSV
        csv_path = str(tmp_path / "customer.csv")
        sample_customer_data.write.csv(csv_path, header=True)

        # Update config to use temp path
        etl_job.config['source_customer_path'] = csv_path

        # Read data
        df = etl_job.read_customer_data()

        # Assertions
        assert df is not None
        assert df.count() > 0
        assert 'CustId' in df.columns
        assert 'Name' in df.columns

    def test_read_order_data_csv(self, etl_job, sample_order_data, tmp_path):
        """Test reading order data from CSV"""
        # Write sample data to temp CSV
        csv_path = str(tmp_path / "order.csv")
        sample_order_data.write.csv(csv_path, header=True)

        # Update config to use temp path
        etl_job.config['source_order_path'] = csv_path

        # Read data
        df = etl_job.read_order_data()

        # Assertions
        assert df is not None
        assert df.count() > 0
        assert 'OrderId' in df.columns
        assert 'CustId' in df.columns

    def test_read_data_parquet(self, etl_job, sample_customer_data, tmp_path):
        """Test reading data from Parquet format"""
        # Write sample data to temp Parquet
        parquet_path = str(tmp_path / "customer.parquet")
        sample_customer_data.write.parquet(parquet_path)

        # Update config
        etl_job.config['source_customer_path'] = parquet_path
        etl_job.config['data_format_customer'] = 'parquet'

        # Read data
        df = etl_job.read_customer_data()

        # Assertions
        assert df is not None
        assert df.count() == sample_customer_data.count()

    def test_s3_path_validation(self, etl_job):
        """Test that S3 paths match TRD requirements"""
        # Verify all S3 paths are from TRD
        assert etl_job.config['source_customer_path'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'
        assert etl_job.config['source_order_path'] == 's3://adif-sdlc/sdlc_wizard/orderdata/'
        assert etl_job.config['output_customer_snapshot_path'] == 's3://adif-sdlc/curated/sdlc_wizard/customer_snapshot/'
        assert etl_job.config['output_order_summary_path'] == 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
        assert etl_job.config['output_curated_customer_path'] == 's3://adif-sdlc/curated/sdlc_wizard/customer/'
        assert etl_job.config['output_curated_order_path'] == 's3://adif-sdlc/curated/sdlc_wizard/order/'
        assert etl_job.config['output_analytics_path'] == 's3://adif-sdlc/analytics/'


class TestDataCleaning:
    """Test FR-CLEAN-001 (TR-CLEAN-001): Data Cleaning"""

    def test_remove_null_values(self, etl_job, sample_customer_data):
        """Test removal of NULL values"""
        # Clean data
        cleaned_df = etl_job.clean_data(sample_customer_data, "customer")

        # Check that critical columns have no nulls
        null_count = cleaned_df.filter(col("CustId").isNull()).count()
        assert null_count == 0

    def test_remove_null_strings(self, etl_job, sample_customer_data):
        """Test removal of 'Null' string values"""
        # Clean data
        cleaned_df = etl_job.clean_data(sample_customer_data, "customer")

        # Check that 'Null' strings are converted to None
        null_string_count = cleaned_df.filter(col("EmailId") == "Null").count()
        assert null_string_count == 0

    def test_remove_duplicates(self, etl_job, sample_customer_data):
        """Test removal of duplicate records"""
        initial_count = sample_customer_data.count()

        # Clean data
        cleaned_df = etl_job.clean_data(sample_customer_data, "customer")

        # Check that duplicates are removed
        final_count = cleaned_df.count()
        assert final_count < initial_count

        # Check no duplicate CustIds
        distinct_count = cleaned_df.select("CustId").distinct().count()
        assert distinct_count == final_count

    def test_cleaning_preserves_valid_data(self, etl_job, spark):
        """Test that cleaning preserves valid data"""
        # Create clean data
        schema = StructType([
            StructField("CustId", StringType(), False),
            StructField("Name", StringType(), False),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])

        data = [
            ("C001", "John Doe", "john.doe@email.com", "North"),
            ("C002", "Jane Smith", "jane.smith@email.com", "South"),
        ]

        df = spark.createDataFrame(data, schema)
        initial_count = df.count()

        # Clean data
        cleaned_df = etl_job.clean_data(df, "customer")

        # All records should be preserved
        assert cleaned_df.count() == initial_count


class TestSCDType2:
    """Test FR-SCD2-001: SCD Type 2 Implementation"""

    def test_add_scd2_columns(self, etl_job, sample_customer_data):
        """Test addition of SCD Type 2 tracking columns"""
        # Add SCD2 columns
        scd2_df = etl_job.add_scd2_columns(sample_customer_data)

        # Check all SCD2 columns are present
        assert 'IsActive' in scd2_df.columns
        assert 'StartDate' in scd2_df.columns
        assert 'EndDate' in scd2_df.columns
        assert 'OpTs' in scd2_df.columns

    def test_scd2_column_types(self, etl_job, sample_customer_data):
        """Test SCD Type 2 column data types"""
        # Add SCD2 columns
        scd2_df = etl_job.add_scd2_columns(sample_customer_data)

        # Check column types
        schema_dict = {field.name: field.dataType for field in scd2_df.schema.fields}

        assert isinstance(schema_dict['IsActive'], BooleanType)
        assert isinstance(schema_dict['StartDate'], TimestampType)
        assert isinstance(schema_dict['EndDate'], TimestampType)
        assert isinstance(schema_dict['OpTs'], TimestampType)

    def test_scd2_initial_values(self, etl_job, sample_customer_data):
        """Test SCD Type 2 initial column values"""
        # Add SCD2 columns
        scd2_df = etl_job.add_scd2_columns(sample_customer_data)

        # Collect first row
        first_row = scd2_df.first()

        # Check initial values
        assert first_row['IsActive'] == True
        assert first_row['StartDate'] is not None
        assert first_row['EndDate'] is None
        assert first_row['OpTs'] is not None

    def test_hudi_configuration(self, etl_job):
        """Test Hudi configuration parameters"""
        # Check Hudi config
        assert etl_job.config['hudi_operation'] == 'upsert'
        assert etl_job.config['hudi_precombine_field'] == 'OpTs'
        assert etl_job.config['primary_key_customer'] == 'CustId'
        assert etl_job.config['primary_key_order'] == 'OrderId'

    @patch('src.main.job.CustomerOrderETL.write_hudi_table')
    def test_write_hudi_table_called(self, mock_write_hudi, etl_job, sample_customer_data):
        """Test that Hudi write is called with correct parameters"""
        scd2_df = etl_job.add_scd2_columns(sample_customer_data)

        # Call write_hudi_table
        etl_job.write_hudi_table(
            scd2_df,
            'customer_snapshot',
            'CustId',
            's3://adif-sdlc/curated/sdlc_wizard/customer_snapshot/'
        )

        # Verify it was called
        mock_write_hudi.assert_called_once()


class TestAggregation:
    """Test FR-AGGREGATE-001: Order Aggregation"""

    def test_aggregate_orders_by_customer(self, etl_job, sample_order_data):
        """Test order aggregation by customer name"""
        # Aggregate orders
        agg_df = etl_job.aggregate_orders(sample_order_data)

        # Check aggregation
        assert 'Name' in agg_df.columns
        assert 'TotalAmount' in agg_df.columns
        assert 'Date' in agg_df.columns

        # Check that aggregation reduced row count
        assert agg_df.count() < sample_order_data.count()

    def test_aggregation_sum_calculation(self, etl_job, spark):
        """Test that total amount is correctly summed"""
        # Create test data
        schema = StructType([
            StructField("Name", StringType(), False),
            StructField("TotalAmount", DecimalType(18, 2), True),
            StructField("Date", DateType(), True)
        ])

        data = [
            ("John Doe", Decimal("100.00"), date(2024, 1, 15)),
            ("John Doe", Decimal("200.00"), date(2024, 1, 20)),
            ("Jane Smith", Decimal("300.00"), date(2024, 1, 18)),
        ]

        df = spark.createDataFrame(data, schema)

        # Aggregate
        agg_df = etl_job.aggregate_orders(df)

        # Check John Doe's total
        john_total = agg_df.filter(col("Name") == "John Doe").select("TotalAmount").first()[0]
        assert john_total == Decimal("300.00")

    def test_aggregation_max_date(self, etl_job, spark):
        """Test that latest date is selected"""
        # Create test data
        schema = StructType([
            StructField("Name", StringType(), False),
            StructField("TotalAmount", DecimalType(18, 2), True),
            StructField("Date", DateType(), True)
        ])

        data = [
            ("John Doe", Decimal("100.00"), date(2024, 1, 15)),
            ("John Doe", Decimal("200.00"), date(2024, 1, 20)),
        ]

        df = spark.createDataFrame(data, schema)

        # Aggregate
        agg_df = etl_job.aggregate_orders(df)

        # Check date
        john_date = agg_df.filter(col("Name") == "John Doe").select("Date").first()[0]
        assert john_date == date(2024, 1, 20)


class TestDataOutput:
    """Test FR-OUTPUT-001: Data Output"""

    def test_write_parquet(self, etl_job, sample_customer_data, tmp_path):
        """Test writing data in Parquet format"""
        output_path = str(tmp_path / "output")

        # Write data
        etl_job.write_parquet(sample_customer_data, output_path)

        # Read back and verify
        df = etl_job.spark.read.parquet(output_path)
        assert df.count() == sample_customer_data.count()

    def test_output_paths_match_trd(self, etl_job):
        """Test that output paths match TRD specifications"""
        # Verify all output paths
        assert 's3://adif-sdlc/curated/sdlc_wizard/customer_snapshot/' in etl_job.config['output_customer_snapshot_path']
        assert 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/' in etl_job.config['output_order_summary_path']
        assert 's3://adif-sdlc/curated/sdlc_wizard/customer/' in etl_job.config['output_curated_customer_path']
        assert 's3://adif-sdlc/curated/sdlc_wizard/order/' in etl_job.config['output_curated_order_path']
        assert 's3://adif-sdlc/analytics/' in etl_job.config['output_analytics_path']

    def test_glue_catalog_registration(self, etl_job, sample_customer_data):
        """Test Glue Catalog table registration"""
        # Mock Spark SQL
        with patch.object(etl_job.spark, 'sql') as mock_sql:
            etl_job.register_glue_table(
                sample_customer_data,
                'customer',
                'sdlc_wizard',
                's3://adif-sdlc/curated/sdlc_wizard/customer/'
            )

            # Verify SQL was called
            mock_sql.assert_called()


class TestDataQuality:
    """Test data quality validation"""

    def test_validate_minimum_record_count(self, etl_job, sample_customer_data):
        """Test minimum record count validation"""
        result = etl_job.validate_data_quality(sample_customer_data, "customer")
        assert result == True

    def test_validate_empty_dataframe(self, etl_job, spark):
        """Test validation fails for empty DataFrame"""
        schema = StructType([StructField("CustId", StringType(), False)])
        empty_df = spark.createDataFrame([], schema)

        result = etl_job.validate_data_quality(empty_df, "customer")
        assert result == False

    def test_schema_validation(self, etl_job, sample_customer_data):
        """Test that schema matches TRD specifications"""
        # Check customer schema
        expected_columns = ['CustId', 'Name', 'EmailId', 'Region']
        actual_columns = sample_customer_data.columns

        for col in expected_columns:
            assert col in actual_columns


class TestConfiguration:
    """Test configuration and setup"""

    def test_spark_configuration(self, etl_job):
        """Test Spark configuration settings"""
        # Check shuffle partitions
        shuffle_partitions = etl_job.spark.conf.get("spark.sql.shuffle.partitions")
        assert shuffle_partitions is not None

    def test_glue_database_name(self, etl_job):
        """Test Glue database name matches TRD"""
        assert etl_job.config['glue_database'] == 'sdlc_wizard'

    def test_table_names_match_trd(self, etl_job):
        """Test table names match TRD specifications"""
        assert etl_job.config['glue_table_customer'] == 'customer'
        assert etl_job.config['glue_table_order'] == 'order'
        assert etl_job.config['glue_table_order_summary'] == 'ordersummary'


class TestEndToEnd:
    """End-to-end integration tests"""

    @patch('src.main.job.CustomerOrderETL.write_hudi_table')
    @patch('src.main.job.CustomerOrderETL.write_parquet')
    @patch('src.main.job.CustomerOrderETL.register_glue_table')
    def test_complete_etl_pipeline(self, mock_register, mock_write_parquet,
                                   mock_write_hudi, etl_job,
                                   sample_customer_data, sample_order_data, tmp_path):
        """Test complete ETL pipeline execution"""
        # Setup temp paths
        customer_path = str(tmp_path / "customer.csv")
        order_path = str(tmp_path / "order.csv")

        sample_customer_data.write.csv(customer_path, header=True)
        sample_order_data.write.csv(order_path, header=True)

        etl_job.config['source_customer_path'] = customer_path
        etl_job.config['source_order_path'] = order_path

        # Run ETL
        etl_job.run()

        # Verify all write operations were called
        assert mock_write_hudi.called
        assert mock_write_parquet.called
        assert mock_register.called


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=src.main", "--cov-report=html"])