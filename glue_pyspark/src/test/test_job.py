"""
Comprehensive tests for AWS Glue PySpark Job
Tests all transformations, data quality operations, and business logic
"""

import sys
import os
from unittest.mock import patch, MagicMock, Mock
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, BooleanType, TimestampType
)
from datetime import datetime


# Setup test Spark session
@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark_session = SparkSession.builder \
        .appName("test_customer_order_job") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Make spark available to job module
    import builtins
    builtins.spark = spark_session

    yield spark_session

    # Don't stop spark in tests
    # spark_session.stop()


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Wilson", "charlie@example.com", "North")
    ]

    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False),
        StructField("emailid", StringType(), False),
        StructField("region", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with nulls for testing cleaning"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "Null", "South"),
        (None, "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "NULL", "charlie@example.com", "North"),
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
    ]

    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data for testing"""
    data = [
        ("O001", "Laptop", Decimal("999.99"), 2, "2024-01-15", "C001"),
        ("O002", "Mouse", Decimal("25.50"), 5, "2024-01-16", "C001"),
        ("O003", "Keyboard", Decimal("75.00"), 3, "2024-01-17", "C002"),
        ("O004", "Monitor", Decimal("299.99"), 1, "2024-01-18", "C003"),
        ("O005", "Headphones", Decimal("150.00"), 2, "2024-01-19", "C002")
    ]

    schema = StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), False),
        StructField("priceperunit", DecimalType(10, 2), False),
        StructField("qty", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("custid", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data_with_nulls(spark):
    """Create sample order data with nulls for testing cleaning"""
    data = [
        ("O001", "Laptop", Decimal("999.99"), 2, "2024-01-15", "C001"),
        ("O002", "Mouse", None, 5, "2024-01-16", "C001"),
        (None, "Keyboard", Decimal("75.00"), 3, "2024-01-17", "C002"),
        ("O004", "Monitor", Decimal("299.99"), 1, "Null", "C003"),
        ("O001", "Laptop", Decimal("999.99"), 2, "2024-01-15", "C001")  # Duplicate
    ]

    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("itemname", StringType(), True),
        StructField("priceperunit", DecimalType(10, 2), True),
        StructField("qty", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("custid", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


class TestConfigManager:
    """Test configuration management"""

    def test_load_config_returns_dict(self):
        """Test that load_config returns a dictionary"""
        from main.job import ConfigManager

        config = ConfigManager.load_config()

        assert isinstance(config, dict)
        assert 'inputs' in config
        assert 'outputs' in config

    def test_get_job_parameters(self):
        """Test getting job parameters"""
        from main.job import ConfigManager

        params = ConfigManager.get_job_parameters()

        assert isinstance(params, dict)
        assert 'inputs' in params
        assert 'customer' in params['inputs']
        assert 'order' in params['inputs']


class TestS3DataReader:
    """Test S3 data reading functionality"""

    def test_read_data_safe_csv(self, spark, sample_customer_data):
        """Test reading CSV data from S3"""
        from main.job import S3DataReader

        reader = S3DataReader(spark)

        # Mock the spark.read.format().option().load() chain
        with patch.object(reader.spark.read, 'format') as mock_format:
            mock_reader = MagicMock()
            mock_format.return_value = mock_reader
            mock_reader.option.return_value = mock_reader
            mock_reader.load.return_value = sample_customer_data

            df = reader.read_data_safe(
                "s3://test-bucket/customer/",
                "csv",
                {"header": "true"}
            )

            assert df is not None
            assert df.count() == 5
            assert "custid" in df.columns

    def test_read_data_safe_normalizes_columns(self, spark, sample_customer_data):
        """Test that column names are normalized to lowercase"""
        from main.job import S3DataReader

        # Create data with uppercase columns
        data = [("C001", "John Doe", "john@example.com", "North")]
        schema = StructType([
            StructField("CustId", StringType(), False),
            StructField("Name", StringType(), False),
            StructField("EmailId", StringType(), False),
            StructField("Region", StringType(), False)
        ])
        df_upper = spark.createDataFrame(data, schema)

        reader = S3DataReader(spark)

        with patch.object(reader.spark.read, 'format') as mock_format:
            mock_reader = MagicMock()
            mock_format.return_value = mock_reader
            mock_reader.option.return_value = mock_reader
            mock_reader.load.return_value = df_upper

            df = reader.read_data_safe("s3://test/", "csv", {})

            # Check all columns are lowercase
            for col in df.columns:
                assert col == col.lower()


class TestS3DataWriter:
    """Test S3 data writing functionality"""

    def test_write_parquet(self, spark, sample_customer_data):
        """Test writing Parquet format"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        # Mock the internal _write_parquet method
        with patch.object(writer, '_write_parquet') as mock_write:
            writer.write_data_safe(
                sample_customer_data,
                "s3://test-bucket/output/",
                "parquet",
                "overwrite"
            )

            mock_write.assert_called_once()

    def test_write_csv(self, spark, sample_customer_data):
        """Test writing CSV format"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        with patch.object(writer, '_write_csv') as mock_write:
            writer.write_data_safe(
                sample_customer_data,
                "s3://test-bucket/output/",
                "csv",
                "overwrite",
                options={"header": "true"}
            )

            mock_write.assert_called_once()

    def test_write_empty_dataframe(self, spark):
        """Test writing empty DataFrame returns False"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        # Create empty DataFrame
        schema = StructType([StructField("col1", StringType(), True)])
        empty_df = spark.createDataFrame([], schema)

        result = writer.write_data_safe(
            empty_df,
            "s3://test-bucket/output/",
            "parquet"
        )

        assert result is False


class TestDataCleaner:
    """Test data cleaning operations"""

    def test_clean_nulls_removes_null_values(self, spark, sample_customer_data_with_nulls):
        """Test that clean_nulls removes NULL values"""
        from main.job import DataCleaner

        df_cleaned = DataCleaner.clean_nulls(sample_customer_data_with_nulls)

        # Should remove rows with None and 'Null'/'NULL' strings
        assert df_cleaned.count() < sample_customer_data_with_nulls.count()

        # Verify no nulls remain
        for row in df_cleaned.collect():
            for value in row:
                assert value is not None
                assert value not in ['Null', 'NULL', 'null']

    def test_remove_duplicates(self, spark, sample_customer_data_with_nulls):
        """Test that remove_duplicates removes duplicate rows"""
        from main.job import DataCleaner

        original_count = sample_customer_data_with_nulls.count()
        df_deduped = DataCleaner.remove_duplicates(sample_customer_data_with_nulls)

        # Should have fewer rows after deduplication
        assert df_deduped.count() < original_count

    def test_clean_dataframe_full_pipeline(self, spark, sample_customer_data_with_nulls):
        """Test full cleaning pipeline"""
        from main.job import DataCleaner

        config = {
            'remove_nulls': True,
            'remove_null_strings': True,
            'remove_duplicates': True,
            'null_string_values': ['Null', 'NULL', 'null']
        }

        df_cleaned = DataCleaner.clean_dataframe(sample_customer_data_with_nulls, config)

        # Should have clean data
        assert df_cleaned.count() > 0
        assert df_cleaned.count() < sample_customer_data_with_nulls.count()


class TestSchemaValidator:
    """Test schema validation and application"""

    def test_get_customer_schema(self):
        """Test customer schema definition"""
        from main.job import SchemaValidator

        schema = SchemaValidator.get_customer_schema()

        assert isinstance(schema, StructType)
        assert len(schema.fields) == 4
        assert schema.fieldNames() == ['custid', 'name', 'emailid', 'region']

    def test_get_order_schema(self):
        """Test order schema definition"""
        from main.job import SchemaValidator

        schema = SchemaValidator.get_order_schema()

        assert isinstance(schema, StructType)
        assert len(schema.fields) == 6
        assert 'orderid' in schema.fieldNames()
        assert 'priceperunit' in schema.fieldNames()

    def test_apply_schema(self, spark, sample_customer_data):
        """Test applying schema to DataFrame"""
        from main.job import SchemaValidator

        schema = SchemaValidator.get_customer_schema()
        df_typed = SchemaValidator.apply_schema(sample_customer_data, schema)

        assert df_typed is not None
        assert df_typed.count() == sample_customer_data.count()


class TestSCD2Manager:
    """Test SCD Type 2 operations"""

    def test_add_scd2_columns(self, spark, sample_customer_data):
        """Test adding SCD2 tracking columns"""
        from main.job import SCD2Manager

        df_scd2 = SCD2Manager.add_scd2_columns(sample_customer_data)

        # Check SCD2 columns are added
        assert 'isactive' in df_scd2.columns
        assert 'startdate' in df_scd2.columns
        assert 'enddate' in df_scd2.columns
        assert 'opts' in df_scd2.columns

        # Check all records are active
        active_count = df_scd2.filter(df_scd2.isactive == True).count()
        assert active_count == df_scd2.count()

        # Check enddate is null for active records
        null_enddate_count = df_scd2.filter(df_scd2.enddate.isNull()).count()
        assert null_enddate_count == df_scd2.count()

    def test_write_hudi_table_disabled(self, spark, sample_customer_data):
        """Test Hudi write when disabled"""
        from main.job import SCD2Manager

        hudi_config = {'enabled': False}

        result = SCD2Manager.write_hudi_table(
            sample_customer_data,
            "s3://test/hudi/",
            hudi_config,
            "test_table"
        )

        assert result is False


class TestAggregationEngine:
    """Test aggregation operations"""

    def test_calculate_customer_aggregate_spend(self, spark, sample_order_data, sample_customer_data):
        """Test customer aggregate spending calculation"""
        from main.job import AggregationEngine

        df_aggregate = AggregationEngine.calculate_customer_aggregate_spend(
            sample_order_data,
            sample_customer_data
        )

        assert df_aggregate is not None
        assert 'name' in df_aggregate.columns
        assert 'date' in df_aggregate.columns
        assert 'totalspend' in df_aggregate.columns

        # Verify aggregation logic
        # John Doe (C001): Laptop (999.99 * 2) + Mouse (25.50 * 5) = 2127.48
        john_spend = df_aggregate.filter(df_aggregate.name == "John Doe").collect()
        assert len(john_spend) > 0

        # Jane Smith (C002): Keyboard (75.00 * 3) + Headphones (150.00 * 2) = 525.00
        jane_spend = df_aggregate.filter(df_aggregate.name == "Jane Smith").collect()
        assert len(jane_spend) > 0

    def test_aggregate_calculation_accuracy(self, spark):
        """Test accuracy of aggregate calculations"""
        from main.job import AggregationEngine

        # Create specific test data
        order_data = [
            ("O001", "Item1", Decimal("100.00"), 2, "2024-01-15", "C001"),
            ("O002", "Item2", Decimal("50.00"), 3, "2024-01-15", "C001")
        ]
        order_schema = StructType([
            StructField("orderid", StringType(), False),
            StructField("itemname", StringType(), False),
            StructField("priceperunit", DecimalType(10, 2), False),
            StructField("qty", IntegerType(), False),
            StructField("date", StringType(), False),
            StructField("custid", StringType(), False)
        ])
        df_order = spark.createDataFrame(order_data, order_schema)

        customer_data = [("C001", "Test Customer", "test@example.com", "North")]
        customer_schema = StructType([
            StructField("custid", StringType(), False),
            StructField("name", StringType(), False),
            StructField("emailid", StringType(), False),
            StructField("region", StringType(), False)
        ])
        df_customer = spark.createDataFrame(customer_data, customer_schema)

        df_aggregate = AggregationEngine.calculate_customer_aggregate_spend(
            df_order,
            df_customer
        )

        result = df_aggregate.collect()[0]
        # Expected: (100.00 * 2) + (50.00 * 3) = 200.00 + 150.00 = 350.00
        assert float(result['totalspend']) == 350.00


class TestCatalogManager:
    """Test Glue Data Catalog operations"""

    def test_register_table(self, spark, sample_customer_data):
        """Test registering table in Glue Data Catalog"""
        from main.job import CatalogManager

        # Mock the SQL execution
        with patch.object(spark, 'sql') as mock_sql:
            result = CatalogManager.register_table(
                spark,
                sample_customer_data,
                "test_database",
                "test_table",
                "s3://test-bucket/data/"
            )

            assert result is True
            mock_sql.assert_called()


class TestIntegration:
    """Integration tests for complete workflows"""

    def test_customer_data_pipeline(self, spark, sample_customer_data_with_nulls):
        """Test complete customer data pipeline"""
        from main.job import DataCleaner, SchemaValidator, SCD2Manager

        # Clean data
        config = {
            'remove_nulls': True,
            'remove_null_strings': True,
            'remove_duplicates': True,
            'null_string_values': ['Null', 'NULL', 'null']
        }
        df_cleaned = DataCleaner.clean_dataframe(sample_customer_data_with_nulls, config)

        # Apply schema
        schema = SchemaValidator.get_customer_schema()
        df_typed = SchemaValidator.apply_schema(df_cleaned, schema)

        # Add SCD2 columns
        df_scd2 = SCD2Manager.add_scd2_columns(df_typed)

        # Verify final result
        assert df_scd2.count() > 0
        assert 'isactive' in df_scd2.columns
        assert 'custid' in df_scd2.columns

    def test_order_data_pipeline(self, spark, sample_order_data_with_nulls):
        """Test complete order data pipeline"""
        from main.job import DataCleaner, SchemaValidator

        # Clean data
        config = {
            'remove_nulls': True,
            'remove_null_strings': True,
            'remove_duplicates': True,
            'null_string_values': ['Null', 'NULL', 'null']
        }
        df_cleaned = DataCleaner.clean_dataframe(sample_order_data_with_nulls, config)

        # Apply schema
        schema = SchemaValidator.get_order_schema()
        df_typed = SchemaValidator.apply_schema(df_cleaned, schema)

        # Verify final result
        assert df_typed.count() > 0
        assert 'orderid' in df_typed.columns
        assert 'priceperunit' in df_typed.columns

    def test_end_to_end_aggregation(self, spark, sample_order_data, sample_customer_data):
        """Test end-to-end aggregation workflow"""
        from main.job import DataCleaner, AggregationEngine

        # Clean both datasets
        config = {
            'remove_nulls': True,
            'remove_duplicates': True
        }
        df_order_clean = DataCleaner.clean_dataframe(sample_order_data, config)
        df_customer_clean = DataCleaner.clean_dataframe(sample_customer_data, config)

        # Calculate aggregates
        df_aggregate = AggregationEngine.calculate_customer_aggregate_spend(
            df_order_clean,
            df_customer_clean
        )

        # Verify results
        assert df_aggregate is not None
        assert df_aggregate.count() > 0
        assert 'totalspend' in df_aggregate.columns

        # Verify all totals are positive
        for row in df_aggregate.collect():
            assert float(row['totalspend']) > 0


class TestSparkContextManager:
    """Test Spark context initialization"""

    def test_initialize_spark_contexts(self, spark):
        """Test Spark context initialization"""
        from main.job import SparkContextManager

        spark_session, glueContext, job, logger = SparkContextManager.initialize_spark_contexts()

        assert spark_session is not None
        assert glueContext is not None
        assert job is not None
        assert logger is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])