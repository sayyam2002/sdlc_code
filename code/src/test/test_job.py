"""
Unit tests for AWS Glue Customer Order Data Pipeline
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()


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
        ("C001", "John Doe", "john@example.com", "North"),  # Duplicate
        ("C004", "Null", "alice@example.com", "West"),  # Contains "Null"
        ("C005", None, "charlie@example.com", "North")  # Contains NULL
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data"""
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", StringType(), True),
        StructField("Qty", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    data = [
        ("O001", "Widget", "10.50", "2", "2024-01-01", "C001"),
        ("O002", "Gadget", "25.00", "1", "2024-01-02", "C002"),
        ("O003", "Tool", "15.75", "3", "2024-01-03", "C003"),
        ("O004", "Widget", "10.50", "5", "2024-01-04", "C001"),
        ("O005", "Null", "20.00", "1", "2024-01-05", "C004"),  # Contains "Null"
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def clean_customer_data(spark):
    """Create clean customer data (no nulls, no duplicates)"""
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def clean_order_data(spark):
    """Create clean order data (no nulls, no duplicates)"""
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", StringType(), True),
        StructField("Qty", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    data = [
        ("O001", "Widget", "10.50", "2", "2024-01-01", "C001"),
        ("O002", "Gadget", "25.00", "1", "2024-01-02", "C002"),
        ("O003", "Tool", "15.75", "3", "2024-01-03", "C003"),
        ("O004", "Widget", "10.50", "5", "2024-01-04", "C001")
    ]
    
    return spark.createDataFrame(data, schema)


class TestS3Reader:
    """Test S3Reader class"""
    
    def test_read_csv(self, spark, mocker):
        """Test CSV reading"""
        from src.main.job import S3Reader
        
        reader = S3Reader(spark)
        mock_df = Mock()
        
        mocker.patch.object(spark.read, 'csv', return_value=mock_df)
        
        result = reader.read_csv("s3://test/path", header=True, infer_schema=True)
        
        assert result == mock_df
        spark.read.csv.assert_called_once_with("s3://test/path", header=True, inferSchema=True)


class TestS3Writer:
    """Test S3Writer class"""
    
    def test_write_parquet(self, spark, mocker):
        """Test Parquet writing"""
        from src.main.job import S3Writer
        
        writer = S3Writer(spark)
        mock_df = Mock()
        mock_write = Mock()
        mock_mode = Mock()
        
        mock_df.write = mock_write
        mock_write.mode.return_value = mock_mode
        mock_mode.parquet.return_value = None
        
        writer._write_parquet(mock_df, "s3://test/path", mode="overwrite")
        
        mock_write.mode.assert_called_once_with("overwrite")
        mock_mode.parquet.assert_called_once_with("s3://test/path")
    
    def test_write_dataframe_parquet(self, spark, mocker):
        """Test write_dataframe with parquet format"""
        from src.main.job import S3Writer
        
        writer = S3Writer(spark)
        mock_df = Mock()
        
        mocker.patch.object(writer, '_write_parquet')
        
        writer.write_dataframe(mock_df, "s3://test/path", format_type="parquet")
        
        writer._write_parquet.assert_called_once_with(mock_df, "s3://test/path", "overwrite")
    
    def test_write_dataframe_hudi(self, spark, mocker):
        """Test write_dataframe with hudi format"""
        from src.main.job import S3Writer
        
        writer = S3Writer(spark)
        mock_df = Mock()
        
        mocker.patch.object(writer, '_write_hudi')
        
        writer.write_dataframe(
            mock_df,
            "s3://test/path",
            format_type="hudi",
            table_name="test_table",
            record_key="id",
            precombine_field="ts"
        )
        
        writer._write_hudi.assert_called_once()


class TestDataCleaner:
    """Test DataCleaner class"""
    
    def test_remove_nulls(self, spark, sample_customer_data):
        """Test null removal"""
        from src.main.job import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.remove_nulls(sample_customer_data)
        
        # Should remove records with "Null" string and NULL values
        assert result.count() == 3  # Only 3 valid records remain
        
        # Verify no "Null" strings remain
        null_strings = result.filter(result.Name == "Null").count()
        assert null_strings == 0
        
        # Verify no NULL values remain
        null_values = result.filter(result.Name.isNull()).count()
        assert null_values == 0
    
    def test_remove_duplicates(self, spark):
        """Test duplicate removal"""
        from src.main.job import DataCleaner
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        data = [
            ("1", "A"),
            ("2", "B"),
            ("1", "A"),  # Duplicate
            ("3", "C")
        ]
        
        df = spark.createDataFrame(data, schema)
        
        cleaner = DataCleaner()
        result = cleaner.remove_duplicates(df)
        
        assert result.count() == 3  # One duplicate removed


class TestSCD2Processor:
    """Test SCD2Processor class"""
    
    def test_apply_scd2_first_load(self, spark, clean_customer_data):
        """Test SCD Type 2 logic for first load"""
        from src.main.job import SCD2Processor
        
        processor = SCD2Processor(spark)
        result = processor.apply_scd2(clean_customer_data, existing_df=None)
        
        # Verify SCD Type 2 fields are added
        assert "IsActive" in result.columns
        assert "StartDate" in result.columns
        assert "EndDate" in result.columns
        assert "OpTs" in result.columns
        
        # Verify all records are active
        active_count = result.filter(result.IsActive == True).count()
        assert active_count == result.count()
        
        # Verify EndDate is null for active records
        null_end_date = result.filter(result.EndDate.isNull()).count()
        assert null_end_date == result.count()


class TestCustomerOrderPipeline:
    """Test CustomerOrderPipeline class"""
    
    def test_clean_customer_data(self, spark, sample_customer_data):
        """Test customer data cleaning"""
        from src.main.job import CustomerOrderPipeline
        
        config = {}
        mock_glue_context = Mock()
        
        pipeline = CustomerOrderPipeline(spark, mock_glue_context, config)
        result = pipeline.clean_customer_data(sample_customer_data)
        
        # Should have 3 clean records (removed 1 duplicate, 1 "Null", 1 NULL)
        assert result.count() == 3
    
    def test_clean_order_data(self, spark, sample_order_data):
        """Test order data cleaning"""
        from src.main.job import CustomerOrderPipeline
        
        config = {}
        mock_glue_context = Mock()
        
        pipeline = CustomerOrderPipeline(spark, mock_glue_context, config)
        result = pipeline.clean_order_data(sample_order_data)
        
        # Should have 4 clean records (removed 1 with "Null")
        assert result.count() == 4
    
    def test_generate_customer_aggregate_spend(self, spark, clean_customer_data, clean_order_data, mocker):
        """Test customer aggregate spend generation"""
        from src.main.job import CustomerOrderPipeline
        
        config = {
            'customeraggregatespend_target_path': 's3://test/agg',
            'catalog_database': 'test_db',
            'customeraggregatespend_table_name': 'test_agg'
        }
        mock_glue_context = Mock()
        
        pipeline = CustomerOrderPipeline(spark, mock_glue_context, config)
        
        # Mock write operations
        mocker.patch.object(pipeline.s3_writer, 'write_dataframe')
        mocker.patch.object(pipeline.catalog_registrar, 'register_table')
        
        result = pipeline.generate_customer_aggregate_spend(clean_customer_data, clean_order_data)
        
        # Verify aggregation columns
        assert "Name" in result.columns
        assert "TotalAmount" in result.columns
        assert "Date" in result.columns
        
        # Verify aggregation logic
        assert result.count() > 0
        
        # Verify write operations were called
        pipeline.s3_writer.write_dataframe.assert_called_once()
        pipeline.catalog_registrar.register_table.assert_called_once()
    
    def test_ingest_customer_data(self, spark, sample_customer_data, mocker):
        """Test customer data ingestion"""
        from src.main.job import CustomerOrderPipeline
        
        config = {
            'customer_source_path': 's3://test/customer',
            'csv_header': True,
            'csv_infer_schema': True
        }
        mock_glue_context = Mock()
        
        pipeline = CustomerOrderPipeline(spark, mock_glue_context, config)
        
        # Mock S3 reader
        mocker.patch.object(pipeline.s3_reader, 'read_csv', return_value=sample_customer_data)
        
        result = pipeline.ingest_customer_data()
        
        assert result.count() == sample_customer_data.count()
        pipeline.s3_reader.read_csv.assert_called_once()
    
    def test_ingest_order_data(self, spark, sample_order_data, mocker):
        """Test order data ingestion"""
        from src.main.job import CustomerOrderPipeline
        
        config = {
            'order_source_path': 's3://test/order',
            'csv_header': True,
            'csv_infer_schema': True
        }
        mock_glue_context = Mock()
        
        pipeline = CustomerOrderPipeline(spark, mock_glue_context, config)
        
        # Mock S3 reader
        mocker.patch.object(pipeline.s3_reader, 'read_csv', return_value=sample_order_data)
        
        result = pipeline.ingest_order_data()
        
        assert result.count() == sample_order_data.count()
        pipeline.s3_reader.read_csv.assert_called_once()


class TestIntegration:
    """Integration tests for the complete pipeline"""
    
    def test_end_to_end_pipeline(self, spark, sample_customer_data, sample_order_data, mocker):
        """Test complete pipeline execution"""
        from src.main.job import CustomerOrderPipeline
        
        config = {
            'customer_source_path': 's3://test/customer',
            'order_source_path': 's3://test/order',
            'customer_catalog_path': 's3://test/catalog/customer',
            'order_catalog_path': 's3://test/catalog/order',
            'ordersummary_target_path': 's3://test/ordersummary',
            'customeraggregatespend_target_path': 's3://test/agg',
            'catalog_database': 'test_db',
            'customer_table_name': 'customer',
            'order_table_name': 'order',
            'customeraggregatespend_table_name': 'agg',
            'hudi_table_name': 'ordersummary',
            'hudi_record_key': 'CustId,OrderId',
            'hudi_precombine_field': 'OpTs'
        }
        mock_glue_context = Mock()
        
        pipeline = CustomerOrderPipeline(spark, mock_glue_context, config)
        
        # Mock all I/O operations
        mocker.patch.object(pipeline.s3_reader, 'read_csv', side_effect=[sample_customer_data, sample_order_data])
        mocker.patch.object(pipeline.s3_writer, 'write_dataframe')
        mocker.patch.object(pipeline.catalog_registrar, 'register_table')
        mocker.patch.object(pipeline.scd2_processor, 'read_existing_hudi', return_value=None)
        
        # Run pipeline
        pipeline.run()
        
        # Verify all steps were executed
        assert pipeline.s3_reader.read_csv.call_count == 2
        assert pipeline.s3_writer.write_dataframe.call_count >= 3
        assert pipeline.catalog_registrar.register_table.call_count >= 3