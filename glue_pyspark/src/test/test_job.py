"""
Unit tests for AWS Glue PySpark ETL Job
Tests all components with mocked Spark I/O operations
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestCustomerOrderETL") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark

    # Don't stop spark if it's a builtin
    import builtins
    if not hasattr(builtins, 'spark'):
        spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
    ]
    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with nulls and duplicates"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", None, "South"),
        ("C003", "Bob Johnson", "bob@example.com", "Null"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C001", "John Doe", "john@example.com", "North"),  # duplicate
    ]
    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data"""
    data = [
        ("O001", "Widget A", 10.50, 2, "2024-01-01", "C001"),
        ("O002", "Widget B", 25.00, 1, "2024-01-02", "C001"),
        ("O003", "Widget C", 15.75, 3, "2024-01-03", "C002"),
        ("O004", "Widget D", 30.00, 2, "2024-01-04", "C003"),
    ]
    schema = StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), True),
        StructField("priceperunit", DoubleType(), True),
        StructField("qty", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("custid", StringType(), False)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data_with_nulls(spark):
    """Create sample order data with nulls and duplicates"""
    data = [
        ("O001", "Widget A", 10.50, 2, "2024-01-01", "C001"),
        ("O002", "Widget B", None, 1, "2024-01-02", "C001"),
        ("O003", "Widget C", 15.75, 3, "Null", "C002"),
        ("O004", "Widget D", 30.00, 2, "2024-01-04", "C003"),
        ("O001", "Widget A", 10.50, 2, "2024-01-01", "C001"),  # duplicate
    ]
    schema = StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), True),
        StructField("priceperunit", DoubleType(), True),
        StructField("qty", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("custid", StringType(), False)
    ])
    return spark.createDataFrame(data, schema)


class TestConfigManager:
    """Test configuration management"""

    def test_get_job_parameters_with_defaults(self):
        """Test that default parameters are returned when config file is missing"""
        from main.job import ConfigManager

        params = ConfigManager.get_job_parameters()

        assert params is not None
        assert "customer_source_path" in params
        assert "order_source_path" in params
        assert "glue_database" in params
        assert params["customer_source_path"] == "s3://adif-sdlc/sdlc_wizard/customerdata/"
        assert params["order_source_path"] == "s3://adif-sdlc/sdlc_wizard/orderdata/"
        assert params["glue_database"] == "gen_ai_poc_databrickscoe"

    def test_get_job_parameters_with_config(self):
        """Test loading parameters from config file"""
        from main.job import ConfigManager

        with patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = """
customer_source_path: "s3://test-bucket/customers/"
order_source_path: "s3://test-bucket/orders/"
glue_database: "test_database"
"""
            params = ConfigManager.get_job_parameters()

            assert params is not None
            assert "customer_source_path" in params


class TestS3DataReader:
    """Test S3 data reading operations"""

    def test_read_data_safe_csv(self, spark, sample_customer_data):
        """Test reading CSV data from S3"""
        from main.job import S3DataReader

        reader = S3DataReader(spark)

        # Mock the internal read method
        with patch.object(reader, '_read_csv_internal', return_value=sample_customer_data):
            df = reader.read_data_safe(
                path="s3://test-bucket/customers/",
                format_type="csv"
            )

            assert df is not None
            assert df.count() == 4
            assert "custid" in df.columns
            assert "name" in df.columns

    def test_read_data_safe_error_handling(self, spark):
        """Test error handling when read fails"""
        from main.job import S3DataReader

        reader = S3DataReader(spark)

        # Mock the internal read method to raise exception
        with patch.object(reader, '_read_csv_internal', side_effect=Exception("S3 read error")):
            with pytest.raises(Exception) as exc_info:
                reader.read_data_safe(
                    path="s3://invalid-bucket/data/",
                    format_type="csv"
                )

            assert "S3 read error" in str(exc_info.value)


class TestS3DataWriter:
    """Test S3 data writing operations"""

    def test_write_data_safe_parquet(self, spark, sample_customer_data):
        """Test writing Parquet data to S3"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        # Mock the internal write method
        with patch.object(writer, '_write_parquet') as mock_write:
            result = writer.write_data_safe(
                df=sample_customer_data,
                path="s3://test-bucket/output/",
                format_type="parquet"
            )

            assert result is True
            mock_write.assert_called_once()

    def test_write_data_safe_hudi(self, spark, sample_order_data):
        """Test writing Hudi data to S3"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        hudi_options = {
            'hoodie.table.name': 'test_table',
            'hoodie.datasource.write.recordkey.field': 'orderid',
            'hoodie.datasource.write.precombine.field': 'date'
        }

        # Mock the internal write method
        with patch.object(writer, '_write_hudi') as mock_write:
            result = writer.write_data_safe(
                df=sample_order_data,
                path="s3://test-bucket/hudi/",
                format_type="hudi",
                hudi_options=hudi_options
            )

            assert result is True
            mock_write.assert_called_once()

    def test_write_data_safe_error_handling(self, spark, sample_customer_data):
        """Test error handling when write fails"""
        from main.job import S3DataWriter

        writer = S3DataWriter(spark)

        # Mock the internal write method to raise exception
        with patch.object(writer, '_write_parquet', side_effect=Exception("S3 write error")):
            with pytest.raises(Exception) as exc_info:
                writer.write_data_safe(
                    df=sample_customer_data,
                    path="s3://invalid-bucket/output/",
                    format_type="parquet"
                )

            assert "S3 write error" in str(exc_info.value)


class TestDataCleaner:
    """Test data cleaning operations"""

    def test_remove_nulls_and_duplicates_customer(self, sample_customer_data_with_nulls):
        """Test removing nulls and duplicates from customer data"""
        from main.job import DataCleaner

        cleaner = DataCleaner()
        cleaned_df = cleaner.remove_nulls_and_duplicates(
            sample_customer_data_with_nulls,
            "customer"
        )

        # Should remove 3 records: 1 with None, 1 with 'Null', 1 duplicate
        assert cleaned_df.count() == 2

        # Verify no nulls remain
        null_count = cleaned_df.filter(
            cleaned_df.emailid.isNull() | (cleaned_df.region == "Null")
        ).count()
        assert null_count == 0

    def test_remove_nulls_and_duplicates_order(self, sample_order_data_with_nulls):
        """Test removing nulls and duplicates from order data"""
        from main.job import DataCleaner

        cleaner = DataCleaner()
        cleaned_df = cleaner.remove_nulls_and_duplicates(
            sample_order_data_with_nulls,
            "order"
        )

        # Should remove 3 records: 1 with None, 1 with 'Null', 1 duplicate
        assert cleaned_df.count() == 2

        # Verify no nulls remain
        null_count = cleaned_df.filter(
            cleaned_df.priceperunit.isNull() | (cleaned_df.date == "Null")
        ).count()
        assert null_count == 0


class TestCatalogManager:
    """Test Glue Data Catalog operations"""

    def test_register_table(self, spark, sample_customer_data):
        """Test registering table in Glue Data Catalog"""
        from main.job import CatalogManager

        catalog = CatalogManager(spark, "test_database")

        # Mock Spark SQL operations
        with patch.object(spark, 'sql') as mock_sql:
            result = catalog.register_table(
                df=sample_customer_data,
                table_name="test_table",
                path="s3://test-bucket/data/"
            )

            assert result is True
            # Verify SQL commands were called
            assert mock_sql.call_count >= 2


class TestHudiManager:
    """Test Hudi operations"""

    def test_prepare_scd_type2_data(self, spark, sample_order_data):
        """Test preparing data for SCD Type 2"""
        from main.job import HudiManager

        hudi_mgr = HudiManager(spark)
        scd_df = hudi_mgr.prepare_scd_type2_data(sample_order_data)

        # Verify SCD columns are added
        assert "IsActive" in scd_df.columns
        assert "StartDate" in scd_df.columns
        assert "EndDate" in scd_df.columns
        assert "OpTs" in scd_df.columns

        # Verify all records are active
        active_count = scd_df.filter(scd_df.IsActive == True).count()
        assert active_count == scd_df.count()

    def test_get_hudi_options(self, spark):
        """Test generating Hudi configuration options"""
        from main.job import HudiManager

        hudi_mgr = HudiManager(spark)
        params = {
            'hudi_record_key': 'OrderId',
            'hudi_precombine_key': 'OpTs',
            'hudi_table_type': 'COPY_ON_WRITE',
            'hudi_operation': 'upsert'
        }

        options = hudi_mgr.get_hudi_options(params, "test_table")

        assert options['hoodie.table.name'] == "test_table"
        assert options['hoodie.datasource.write.recordkey.field'] == 'OrderId'
        assert options['hoodie.datasource.write.precombine.field'] == 'OpTs'
        assert options['hoodie.datasource.write.table.type'] == 'COPY_ON_WRITE'


class TestAggregationEngine:
    """Test aggregation operations"""

    def test_calculate_customer_aggregate_spend(self, spark, sample_order_data):
        """Test calculating customer aggregate spend"""
        from main.job import AggregationEngine

        agg_df = AggregationEngine.calculate_customer_aggregate_spend(sample_order_data)

        # Verify aggregation results
        assert agg_df.count() == 3  # 3 unique customers
        assert "CustId" in agg_df.columns
        assert "TotalSpend" in agg_df.columns

        # Verify specific customer totals
        c001_spend = agg_df.filter(agg_df.CustId == "C001").collect()[0]["TotalSpend"]
        assert c001_spend == (10.50 * 2) + (25.00 * 1)  # 46.0

        c002_spend = agg_df.filter(agg_df.CustId == "C002").collect()[0]["TotalSpend"]
        assert c002_spend == 15.75 * 3  # 47.25


class TestSparkContextManager:
    """Test Spark context initialization"""

    def test_initialize_spark_contexts(self):
        """Test initializing Spark contexts"""
        from main.job import SparkContextManager

        spark, glueContext, job = SparkContextManager.initialize_spark_contexts()

        assert spark is not None
        assert glueContext is not None
        assert job is not None


class TestEndToEndIntegration:
    """End-to-end integration tests"""

    def test_full_etl_pipeline(self, spark, sample_customer_data, sample_order_data):
        """Test complete ETL pipeline with mocked I/O"""
        from main.job import (
            S3DataReader, S3DataWriter, DataCleaner,
            CatalogManager, HudiManager, AggregationEngine
        )

        # Initialize components
        reader = S3DataReader(spark)
        writer = S3DataWriter(spark)
        cleaner = DataCleaner()
        catalog = CatalogManager(spark, "test_database")
        hudi_mgr = HudiManager(spark)

        # Mock read operations
        with patch.object(reader, '_read_csv_internal', side_effect=[sample_customer_data, sample_order_data]):
            # Read data
            customer_df = reader.read_data_safe("s3://test/customers/", "csv")
            order_df = reader.read_data_safe("s3://test/orders/", "csv")

            assert customer_df.count() == 4
            assert order_df.count() == 4

        # Clean data
        customer_cleaned = cleaner.remove_nulls_and_duplicates(customer_df, "customer")
        order_cleaned = cleaner.remove_nulls_and_duplicates(order_df, "order")

        assert customer_cleaned.count() == 4
        assert order_cleaned.count() == 4

        # Prepare SCD Type 2 data
        order_scd = hudi_mgr.prepare_scd_type2_data(order_cleaned)
        assert "IsActive" in order_scd.columns

        # Calculate aggregations
        agg_df = AggregationEngine.calculate_customer_aggregate_spend(order_cleaned)
        assert agg_df.count() == 3

        # Mock write operations
        with patch.object(writer, '_write_parquet') as mock_parquet:
            writer.write_data_safe(customer_cleaned, "s3://test/output/", "parquet")
            assert mock_parquet.called

        # Mock catalog operations
        with patch.object(spark, 'sql'):
            catalog.register_table(customer_cleaned, "test_table", "s3://test/output/")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])