"""
Comprehensive test suite for Customer Order Data Processing Pipeline
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType
)
import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestCustomerOrderProcessor") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Davis", "charlie@example.com", "North")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("OrderId", StringType(), True)
    ])

    data = [
        ("C001", "O001"),
        ("C001", "O002"),
        ("C002", "O003"),
        ("C003", "O004"),
        ("C004", "O005")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_data_with_nulls(spark):
    """Create sample data with NULL and 'Null' values"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),
        ("C003", "Bob Johnson", "Null", "East"),
        (None, "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Davis", "charlie@example.com", "Null")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_data_with_duplicates(spark):
    """Create sample data with duplicates"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East")
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_processor():
    """Create mock processor with mocked AWS services"""
    with patch('src.job.SparkContext') as mock_sc, \
         patch('src.job.GlueContext') as mock_glue, \
         patch('src.job.Job') as mock_job:

        from src.job import CustomerOrderProcessor

        # Mock Spark session
        mock_spark = MagicMock()
        mock_glue_instance = MagicMock()
        mock_glue_instance.spark_session = mock_spark
        mock_glue.return_value = mock_glue_instance

        processor = CustomerOrderProcessor()
        yield processor


class TestFRIngest001:
    """Test FR-INGEST-001: Data Ingestion from S3"""

    def test_customer_data_path_matches_trd(self):
        """Verify customer data S3 path matches TRD"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.CUSTOMER_DATA_PATH == "s3://adif-sdlc/sdlc_wizard/customerdata/"

    def test_order_data_path_matches_trd(self):
        """Verify order data S3 path matches TRD"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.ORDER_DATA_PATH == "s3://adif-sdlc/sdlc_wizard/orderdata/"

    def test_read_customer_data_with_utf8_encoding(self, spark, tmp_path):
        """Test reading customer data with UTF-8 encoding"""
        from src.job import CustomerOrderProcessor

        # Create test CSV file
        test_file = tmp_path / "customer.csv"
        test_file.write_text("CustId,Name,EmailId,Region\nC001,John Doe,john@example.com,North\n", encoding='utf-8')

        processor = CustomerOrderProcessor()
        processor.spark = spark
        processor.CUSTOMER_DATA_PATH = str(tmp_path)

        df = processor.read_customer_data()
        assert df.count() == 1
        assert "CustId" in df.columns


class TestFRClean001:
    """Test FR-CLEAN-001: Data Cleaning Requirements"""

    def test_remove_null_values(self, spark, sample_data_with_nulls):
        """Test removal of NULL values"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        cleaned_df = processor.clean_data(sample_data_with_nulls)

        # Should only have 1 record (C001) without any NULL or 'Null'
        assert cleaned_df.count() == 1
        assert cleaned_df.first()["CustId"] == "C001"

    def test_remove_null_string_values(self, spark, sample_data_with_nulls):
        """Test removal of 'Null' string values"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        cleaned_df = processor.clean_data(sample_data_with_nulls)

        # Verify no 'Null' strings remain
        for row in cleaned_df.collect():
            for col_name in cleaned_df.columns:
                assert row[col_name] != "Null"

    def test_remove_duplicates(self, spark, sample_data_with_duplicates):
        """Test removal of duplicate records"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        cleaned_df = processor.clean_data(sample_data_with_duplicates)

        # Should have 3 unique records
        assert cleaned_df.count() == 3

    def test_filter_condition_all_columns(self, spark, sample_data_with_nulls):
        """Test filter condition applies to all columns"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        cleaned_df = processor.clean_data(sample_data_with_nulls)

        # Verify all remaining records have no NULL or 'Null' in any column
        for row in cleaned_df.collect():
            for col_name in cleaned_df.columns:
                assert row[col_name] is not None
                assert row[col_name] != "Null"


class TestFRSCD2001:
    """Test FR-SCD2-001: SCD Type 2 Implementation"""

    def test_add_isactive_column(self, spark, sample_customer_data):
        """Test IsActive column is added"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        scd_df = processor.add_scd_type2_columns(sample_customer_data)

        assert "IsActive" in scd_df.columns
        assert all(row["IsActive"] == True for row in scd_df.collect())

    def test_add_startdate_column(self, spark, sample_customer_data):
        """Test StartDate column is added"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        scd_df = processor.add_scd_type2_columns(sample_customer_data)

        assert "StartDate" in scd_df.columns
        assert all(row["StartDate"] is not None for row in scd_df.collect())

    def test_add_enddate_column(self, spark, sample_customer_data):
        """Test EndDate column is added"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        scd_df = processor.add_scd_type2_columns(sample_customer_data)

        assert "EndDate" in scd_df.columns
        assert all(row["EndDate"] is None for row in scd_df.collect())

    def test_add_opts_column(self, spark, sample_customer_data):
        """Test OpTs column is added"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        scd_df = processor.add_scd_type2_columns(sample_customer_data)

        assert "OpTs" in scd_df.columns
        assert all(row["OpTs"] is not None for row in scd_df.collect())


class TestFRTransform001:
    """Test FR-TRANSFORM-001: Data Transformation Requirements"""

    def test_join_customer_order_data(self, spark, sample_customer_data, sample_order_data):
        """Test joining customer and order data"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        joined_df = processor.join_customer_order_data(sample_customer_data, sample_order_data)

        assert joined_df.count() == 5
        assert "CustId" in joined_df.columns
        assert "OrderId" in joined_df.columns
        assert "Name" in joined_df.columns

    def test_join_preserves_customer_columns(self, spark, sample_customer_data, sample_order_data):
        """Test join preserves all customer columns"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        joined_df = processor.join_customer_order_data(sample_customer_data, sample_order_data)

        assert "Name" in joined_df.columns
        assert "EmailId" in joined_df.columns
        assert "Region" in joined_df.columns


class TestHudiConfiguration:
    """Test Hudi Configuration Requirements"""

    def test_hudi_table_name_configuration(self):
        """Test Hudi table name matches TRD"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.HUDI_TABLE_NAME == "ordersummary"

    def test_hudi_record_key_configuration(self):
        """Test Hudi record key includes CustId and OrderId"""
        from src.job import CustomerOrderProcessor
        assert "CustId" in CustomerOrderProcessor.HUDI_RECORD_KEY
        assert "OrderId" in CustomerOrderProcessor.HUDI_RECORD_KEY

    def test_hudi_precombine_field_configuration(self):
        """Test Hudi precombine field is OpTs"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.HUDI_PRECOMBINE_FIELD == "OpTs"

    def test_hudi_partition_field_configuration(self):
        """Test Hudi partition field is Region"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.HUDI_PARTITION_FIELD == "Region"

    def test_hudi_output_path_matches_trd(self):
        """Test Hudi output path matches TRD"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.ORDER_SUMMARY_OUTPUT_PATH == "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"


class TestGlueCatalogIntegration:
    """Test AWS Glue Catalog Integration"""

    def test_glue_database_name_matches_trd(self):
        """Test Glue database name matches TRD"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.GLUE_DATABASE == "sdlc_wizard"

    def test_glue_table_name_matches_trd(self):
        """Test Glue table name matches TRD"""
        from src.job import CustomerOrderProcessor
        assert CustomerOrderProcessor.GLUE_TABLE_ORDER_SUMMARY == "ordersummary"


class TestSchemaValidation:
    """Test Schema Validation Against TRD"""

    def test_customer_schema_columns(self, sample_customer_data):
        """Test customer schema matches TRD"""
        expected_columns = ["CustId", "Name", "EmailId", "Region"]
        assert all(col in sample_customer_data.columns for col in expected_columns)

    def test_order_schema_columns(self, sample_order_data):
        """Test order schema matches TRD"""
        expected_columns = ["CustId", "OrderId"]
        assert all(col in sample_order_data.columns for col in expected_columns)

    def test_scd_schema_columns(self, spark, sample_customer_data):
        """Test SCD Type 2 schema includes all required columns"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        scd_df = processor.add_scd_type2_columns(sample_customer_data)

        scd_columns = ["IsActive", "StartDate", "EndDate", "OpTs"]
        assert all(col in scd_df.columns for col in scd_columns)


class TestErrorHandling:
    """Test Error Handling and Logging"""

    def test_read_customer_data_error_handling(self, spark):
        """Test error handling when reading customer data fails"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark
        processor.CUSTOMER_DATA_PATH = "s3://invalid-bucket/invalid-path/"

        with pytest.raises(Exception):
            processor.read_customer_data()

    def test_clean_data_error_handling(self, spark):
        """Test error handling during data cleaning"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        # Pass None to trigger error
        with pytest.raises(Exception):
            processor.clean_data(None)


class TestEndToEndPipeline:
    """Test End-to-End Pipeline Execution"""

    def test_pipeline_integration(self, spark, sample_customer_data, sample_order_data):
        """Test complete pipeline integration"""
        from src.job import CustomerOrderProcessor

        processor = CustomerOrderProcessor()
        processor.spark = spark

        # Clean data
        customer_cleaned = processor.clean_data(sample_customer_data)
        order_cleaned = processor.clean_data(sample_order_data)

        # Join data
        joined = processor.join_customer_order_data(customer_cleaned, order_cleaned)

        # Add SCD columns
        scd_df = processor.add_scd_type2_columns(joined)

        # Verify final schema
        assert "CustId" in scd_df.columns
        assert "OrderId" in scd_df.columns
        assert "IsActive" in scd_df.columns
        assert "StartDate" in scd_df.columns
        assert "EndDate" in scd_df.columns
        assert "OpTs" in scd_df.columns

        # Verify data integrity
        assert scd_df.count() > 0