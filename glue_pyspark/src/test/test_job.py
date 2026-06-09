"""
Unit tests for AWS Glue PySpark ETL Job
"""

import sys
from unittest.mock import patch, MagicMock, mock_open
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pytest


# Initialize Spark session for testing
@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_job") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    # Don't stop spark if it exists in builtins
    if not hasattr(__builtins__, 'spark'):
        spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
    ]
    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data"""
    data = [
        ("O001", "Widget", "10.50", "2", "2024-01-01", "C001"),
        ("O002", "Gadget", "25.00", "1", "2024-01-01", "C001"),
        ("O003", "Tool", "15.75", "3", "2024-01-02", "C002"),
        ("O004", "Device", "50.00", "1", "2024-01-02", "C003"),
    ]
    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("itemname", StringType(), True),
        StructField("priceperunit", StringType(), True),
        StructField("qty", StringType(), True),
        StructField("date", StringType(), True),
        StructField("custid", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with nulls"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        (None, "Alice Brown", "alice@example.com", "West"),
    ]
    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_duplicates(spark):
    """Create sample customer data with duplicates"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
        ("C003", "Bob Johnson", "bob@example.com", "East"),
    ]
    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_config():
    """Mock configuration data"""
    return {
        "customer_source_path": "s3://adif-sdlc/sdlc_wizard/customerdata/",
        "customer_source_format": "csv",
        "customer_source_options": {
            "header": "true",
            "inferSchema": "true",
            "delimiter": ","
        },
        "order_source_path": "s3://adif-sdlc/sdlc_wizard/orderdata/",
        "order_source_format": "csv",
        "order_source_options": {
            "header": "true",
            "inferSchema": "true",
            "delimiter": ","
        },
        "customer_target_path": "s3://adif-sdlc/catalog/sdlc_wizard/customer/",
        "customer_target_format": "parquet",
        "customer_target_mode": "overwrite",
        "order_target_path": "s3://adif-sdlc/catalog/sdlc_wizard/order/",
        "order_target_format": "parquet",
        "order_target_mode": "overwrite",
        "aggregate_target_path": "s3://adif-sdlc/analytics/customeraggregatespend/",
        "aggregate_target_format": "parquet",
        "aggregate_target_mode": "overwrite",
        "glue_database": "gen_ai_poc_databrickscoe",
        "customer_table_name": "customer",
        "order_table_name": "order",
        "aggregate_table_name": "customeraggregatespend",
        "customer_expected_columns": ["custid", "name", "emailid", "region"],
        "order_expected_columns": ["orderid", "itemname", "priceperunit", "qty", "date", "custid"],
        "enable_data_cleaning": True,
        "remove_nulls": True,
        "remove_duplicates": True
    }


class TestConfigManager:
    """Test ConfigManager class"""

    def test_get_job_parameters(self, mock_config):
        """Test getting job parameters from config"""
        from main.job import ConfigManager

        yaml_content = """
customer_source_path: "s3://adif-sdlc/sdlc_wizard/customerdata/"
customer_source_format: "csv"
glue_database: "gen_ai_poc_databrickscoe"
"""

        with patch("builtins.open", mock_open(read_data=yaml_content)):
            params = ConfigManager.get_job_parameters()
            assert "customer_source_path" in params
            assert params["customer_source_path"] == "s3://adif-sdlc/sdlc_wizard/customerdata/"


class TestS3DataReader:
    """Test S3DataReader class"""

    def test_read_data_safe_csv_success(self, spark, sample_customer_data, mock_config):
        """Test successful CSV read"""
        from main.job import S3DataReader

        reader = S3DataReader(spark)

        with patch.object(reader, '_read_csv', return_value=sample_customer_data):
            df = reader.read_data_safe(
                path=mock_config["customer_source_path"],
                format_type="csv",
                options=mock_config["customer_source_options"],
                expected_columns=mock_config["customer_expected_columns"]
            )

            assert df.count() == 3
            assert set(df.columns) == {"custid", "name", "emailid", "region"}

    def test_read_data_safe_schema_validation_failure(self, spark, sample_customer_data):
        """Test schema validation failure"""
        from main.job import S3DataReader

        reader = S3DataReader(spark)

        with patch.object(reader, '_read_csv', return_value=sample_customer_data):
            with pytest.raises(Exception) as exc_info:
                reader.read_data_safe(
                    path="s3://test/path/",
                    format_type="csv",
                    options={},
                    expected_columns=["custid", "name", "emailid", "region", "missing_column"]
                )

            assert "Schema validation failed" in str(exc_info.value)

    def test_read_data_safe_empty_path(self, spark):
        """Test empty path validation"""
        from main.job import S3DataReader

        reader = S3DataReader(spark)

        with pytest.raises(ValueError) as exc_info:
            reader.read_data_safe(
                path="",
                format_type="csv",
                options={}
            )

        assert "empty or invalid" in str(exc_info.value)


class TestS3DataWriter:
    """Test S3DataWriter class"""

    def test_write_data_safe_parquet(self, spark, sample_customer_data):
        """Test writing Parquet data"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        with patch.object(writer, '_write_parquet') as mock_write:
            writer.write_data_safe(
                df=sample_customer_data,
                path="s3://test/output/",
                format_type="parquet",
                mode="overwrite"
            )

            mock_write.assert_called_once()
            args = mock_write.call_args[0]
            assert args[1] == "s3://test/output/"
            assert args[2] == "overwrite"

    def test_write_data_safe_csv(self, spark, sample_customer_data):
        """Test writing CSV data"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        with patch.object(writer, '_write_csv') as mock_write:
            writer.write_data_safe(
                df=sample_customer_data,
                path="s3://test/output/",
                format_type="csv",
                mode="append"
            )

            mock_write.assert_called_once()
            args = mock_write.call_args[0]
            assert args[1] == "s3://test/output/"
            assert args[2] == "append"


class TestDataCleaner:
    """Test DataCleaner class"""

    def test_remove_nulls_and_null_strings(self, spark, sample_customer_data_with_nulls):
        """Test removing NULL values and 'Null' strings"""
        from main.job import DataCleaner

        df_cleaned = DataCleaner.remove_nulls_and_null_strings(sample_customer_data_with_nulls)

        # Only the first row should remain (no nulls or "Null" strings)
        assert df_cleaned.count() == 1

        row = df_cleaned.first()
        assert row.custid == "C001"
        assert row.name == "John Doe"

    def test_remove_duplicates(self, spark, sample_customer_data_with_duplicates):
        """Test removing duplicate records"""
        from main.job import DataCleaner

        df_deduped = DataCleaner.remove_duplicates(sample_customer_data_with_duplicates)

        # Should have 3 unique records
        assert df_deduped.count() == 3

    def test_clean_dataframe(self, spark, sample_customer_data_with_nulls):
        """Test complete cleaning pipeline"""
        from main.job import DataCleaner

        df_cleaned = DataCleaner.clean_dataframe(sample_customer_data_with_nulls, enable_cleaning=True)

        # Should remove rows with nulls
        assert df_cleaned.count() == 1

    def test_clean_dataframe_disabled(self, spark, sample_customer_data_with_nulls):
        """Test cleaning disabled"""
        from main.job import DataCleaner

        df_cleaned = DataCleaner.clean_dataframe(sample_customer_data_with_nulls, enable_cleaning=False)

        # Should not remove any rows
        assert df_cleaned.count() == 4


class TestGlueCatalogManager:
    """Test GlueCatalogManager class"""

    def test_register_table(self, spark, sample_customer_data):
        """Test registering table in Glue Catalog"""
        from main.job import GlueCatalogManager

        catalog_manager = GlueCatalogManager(spark, "test_database")

        with patch.object(spark, 'sql') as mock_sql:
            # Mock the write operation
            mock_writer = MagicMock()
            with patch.object(sample_customer_data, 'write', mock_writer):
                catalog_manager.register_table(
                    df=sample_customer_data,
                    table_name="test_table",
                    path="s3://test/path/"
                )

                # Verify database creation was attempted
                assert any("CREATE DATABASE" in str(call) for call in mock_sql.call_args_list)

                # Verify table drop was attempted
                assert any("DROP TABLE" in str(call) for call in mock_sql.call_args_list)


class TestDataAggregator:
    """Test DataAggregator class"""

    def test_calculate_customer_aggregate_spend(self, spark, sample_customer_data, sample_order_data):
        """Test customer aggregate spend calculation"""
        from main.job import DataAggregator

        df_aggregate = DataAggregator.calculate_customer_aggregate_spend(
            df_customer=sample_customer_data,
            df_order=sample_order_data
        )

        # Should have aggregated data
        assert df_aggregate.count() > 0

        # Check schema
        expected_columns = {"custid", "name", "emailid", "region", "date", "totalspend"}
        assert set(df_aggregate.columns) == expected_columns

        # Verify calculation for C001 on 2024-01-01
        # O001: 10.50 * 2 = 21.00
        # O002: 25.00 * 1 = 25.00
        # Total: 46.00
        c001_data = df_aggregate.filter(
            (df_aggregate.custid == "C001") & (df_aggregate.date == "2024-01-01")
        ).first()

        assert c001_data is not None
        assert abs(c001_data.totalspend - 46.00) < 0.01


class TestMainFunction:
    """Test main ETL function"""

    def test_main_execution(self, spark, sample_customer_data, sample_order_data, mock_config):
        """Test main function execution"""
        from main.job import main, ConfigManager, S3DataReader, S3DataWriter, GlueCatalogManager

        with patch('main.job.SparkContextManager.initialize_spark_contexts', return_value=(spark, MagicMock(), None)):
            with patch.object(ConfigManager, 'get_job_parameters', return_value=mock_config):
                with patch.object(S3DataReader, '_read_csv') as mock_read:
                    # Return customer data first, then order data
                    mock_read.side_effect = [sample_customer_data, sample_order_data]

                    with patch.object(S3DataWriter, '_write_parquet') as mock_write:
                        with patch.object(GlueCatalogManager, 'register_table'):
                            # Execute main function
                            main()

                            # Verify reads were called
                            assert mock_read.call_count == 2

                            # Verify writes were called (customer, order, aggregate)
                            assert mock_write.call_count == 3


class TestColumnNormalization:
    """Test column name normalization"""

    def test_column_normalization(self, spark):
        """Test that column names are normalized to lowercase"""
        from main.job import S3DataReader

        # Create DataFrame with mixed case columns
        data = [("C001", "John Doe", "john@example.com", "North")]
        schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True),
        ])
        df_mixed_case = spark.createDataFrame(data, schema)

        reader = S3DataReader(spark)

        with patch.object(reader, '_read_csv', return_value=df_mixed_case):
            df = reader.read_data_safe(
                path="s3://test/path/",
                format_type="csv",
                options={}
            )

            # All columns should be lowercase
            assert set(df.columns) == {"custid", "name", "emailid", "region"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])