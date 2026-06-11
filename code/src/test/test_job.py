"""
Unit tests for AWS Glue ETL Job
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from main.job import (
    S3Reader, S3Writer, DataCleaner, SCD2Processor, 
    CatalogRegistrar, GlueETLJob
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
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
        ("C003", "Bob Wilson", "bob@example.com", "East"),
        ("C004", "Null", "alice@example.com", "West"),  # Has "Null" string
        ("C005", "Charlie Brown", None, "North"),  # Has NULL value
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
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
        ("O004", "Null", "20.00", "1", "2024-01-04", "C001"),  # Has "Null" string
        ("O005", "Device", None, "2", "2024-01-05", "C002")  # Has NULL value
    ]
    
    return spark.createDataFrame(data, schema)


class TestS3Reader:
    """Test S3Reader class"""
    
    def test_read_csv(self, spark, tmp_path):
        """Test CSV reading"""
        # Create test CSV file
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("CustId,Name\nC001,John\nC002,Jane")
        
        reader = S3Reader(spark)
        df = reader.read_csv(str(tmp_path), header=True, infer_schema=True)
        
        assert df.count() == 2
        assert "CustId" in df.columns
        assert "Name" in df.columns
    
    def test_read_hudi_not_exists(self, spark):
        """Test reading non-existent Hudi table"""
        reader = S3Reader(spark)
        result = reader.read_hudi("/non/existent/path")
        
        assert result is None


class TestS3Writer:
    """Test S3Writer class"""
    
    def test_write_parquet(self, spark, sample_customer_data, tmp_path, mocker):
        """Test Parquet writing"""
        writer = S3Writer(spark)
        output_path = str(tmp_path / "output")
        
        # Mock the internal _write_parquet method
        mock_write = mocker.patch.object(writer, '_write_parquet')
        
        writer.write_dataframe(
            sample_customer_data,
            output_path,
            format_type="parquet",
            mode="overwrite"
        )
        
        mock_write.assert_called_once()
    
    def test_write_hudi(self, spark, sample_customer_data, tmp_path, mocker):
        """Test Hudi writing"""
        writer = S3Writer(spark)
        output_path = str(tmp_path / "hudi_output")
        
        hudi_config = {
            'table_name': 'test_table',
            'record_key': 'CustId',
            'precombine_key': 'Name',
            'table_type': 'COPY_ON_WRITE'
        }
        
        # Mock the internal _write_hudi method
        mock_write = mocker.patch.object(writer, '_write_hudi')
        
        writer.write_dataframe(
            sample_customer_data,
            output_path,
            format_type="hudi",
            hudi_config=hudi_config
        )
        
        mock_write.assert_called_once()
    
    def test_write_unsupported_format(self, spark, sample_customer_data, tmp_path):
        """Test writing with unsupported format"""
        writer = S3Writer(spark)
        
        with pytest.raises(ValueError, match="Unsupported format"):
            writer.write_dataframe(
                sample_customer_data,
                str(tmp_path),
                format_type="invalid_format"
            )


class TestDataCleaner:
    """Test DataCleaner class"""
    
    def test_remove_nulls(self, spark, sample_customer_data):
        """Test null removal"""
        cleaner = DataCleaner()
        cleaned = cleaner.remove_nulls(sample_customer_data, null_string="Null")
        
        # Should remove records with "Null" string and NULL values
        assert cleaned.count() < sample_customer_data.count()
        
        # Verify no "Null" strings remain
        null_string_count = cleaned.filter(cleaned.Name == "Null").count()
        assert null_string_count == 0
    
    def test_remove_duplicates(self, spark, sample_customer_data):
        """Test duplicate removal"""
        cleaner = DataCleaner()
        deduped = cleaner.remove_duplicates(sample_customer_data)
        
        # Should remove duplicate records
        assert deduped.count() < sample_customer_data.count()
        
        # Verify no duplicates remain
        distinct_count = deduped.select("CustId", "Name", "EmailId", "Region").distinct().count()
        assert distinct_count == deduped.count()
    
    def test_full_cleaning_pipeline(self, spark, sample_customer_data):
        """Test complete cleaning pipeline"""
        cleaner = DataCleaner()
        
        # Remove nulls first
        no_nulls = cleaner.remove_nulls(sample_customer_data, null_string="Null")
        
        # Then remove duplicates
        cleaned = cleaner.remove_duplicates(no_nulls)
        
        # Should have only clean, unique records
        assert cleaned.count() == 3  # C001, C002, C003 (after removing nulls and duplicates)


class TestSCD2Processor:
    """Test SCD2Processor class"""
    
    def test_process_scd2_initial_load(self, spark, sample_customer_data):
        """Test SCD2 processing for initial load"""
        processor = SCD2Processor(spark)
        
        # Clean data first
        cleaner = DataCleaner()
        clean_data = cleaner.remove_nulls(sample_customer_data)
        clean_data = cleaner.remove_duplicates(clean_data)
        
        # Process SCD2 with no existing data
        result = processor.process_scd2(clean_data, None, ["CustId"])
        
        # Verify SCD2 columns added
        assert "IsActive" in result.columns
        assert "StartDate" in result.columns
        assert "EndDate" in result.columns
        assert "OpTs" in result.columns
        
        # All records should be active
        active_count = result.filter(result.IsActive == True).count()
        assert active_count == result.count()
    
    def test_process_scd2_with_existing(self, spark, sample_customer_data):
        """Test SCD2 processing with existing data"""
        processor = SCD2Processor(spark)
        
        # Clean data
        cleaner = DataCleaner()
        clean_data = cleaner.remove_nulls(sample_customer_data)
        clean_data = cleaner.remove_duplicates(clean_data)
        
        # Create mock existing data
        existing = processor.process_scd2(clean_data, None, ["CustId"])
        
        # Process with existing data
        result = processor.process_scd2(clean_data, existing, ["CustId"])
        
        # Should have SCD2 metadata
        assert "IsActive" in result.columns
        assert result.count() > 0


class TestGlueETLJob:
    """Test GlueETLJob class"""
    
    @patch('main.job.SparkContext')
    @patch('main.job.GlueContext')
    @patch('main.job.Job')
    def test_job_initialization(self, mock_job, mock_glue_context, mock_spark_context):
        """Test job initialization"""
        args = {
            'JOB_NAME': 'test_job',
            'source_customer_path': 's3://test/customer/',
            'source_order_path': 's3://test/order/'
        }
        
        # Mock Spark session
        mock_spark = MagicMock()
        mock_glue_context.return_value.spark_session = mock_spark
        
        job = GlueETLJob(args)
        
        assert job.config['source_customer_path'] == 's3://test/customer/'
        assert job.config['source_order_path'] == 's3://test/order/'
    
    @patch('main.job.SparkContext')
    @patch('main.job.GlueContext')
    @patch('main.job.Job')
    def test_clean_data(self, mock_job, mock_glue_context, mock_spark_context, 
                       spark, sample_customer_data):
        """Test data cleaning in job"""
        args = {'JOB_NAME': 'test_job'}
        
        # Mock Spark session
        mock_glue_context.return_value.spark_session = spark
        
        job = GlueETLJob(args)
        
        # Clean data
        cleaned = job.clean_data(sample_customer_data, "customer")
        
        # Should have fewer records after cleaning
        assert cleaned.count() < sample_customer_data.count()
        assert cleaned.count() == 3  # Only 3 valid unique records


class TestIntegration:
    """Integration tests"""
    
    def test_end_to_end_cleaning(self, spark, sample_customer_data, sample_order_data):
        """Test end-to-end data cleaning"""
        cleaner = DataCleaner()
        
        # Clean customer data
        customer_clean = cleaner.remove_nulls(sample_customer_data)
        customer_clean = cleaner.remove_duplicates(customer_clean)
        
        # Clean order data
        order_clean = cleaner.remove_nulls(sample_order_data)
        order_clean = cleaner.remove_duplicates(order_clean)
        
        # Verify results
        assert customer_clean.count() == 3
        assert order_clean.count() == 3
        
        # Join cleaned data
        joined = order_clean.join(customer_clean, "CustId", "inner")
        assert joined.count() > 0
    
    def test_scd2_workflow(self, spark, sample_customer_data, sample_order_data):
        """Test SCD2 workflow"""
        cleaner = DataCleaner()
        processor = SCD2Processor(spark)
        
        # Clean data
        customer_clean = cleaner.remove_nulls(sample_customer_data)
        customer_clean = cleaner.remove_duplicates(customer_clean)
        order_clean = cleaner.remove_nulls(sample_order_data)
        order_clean = cleaner.remove_duplicates(order_clean)
        
        # Join
        joined = order_clean.join(customer_clean, "CustId", "inner")
        
        # Process SCD2
        scd2_result = processor.process_scd2(joined, None, ["OrderId", "CustId"])
        
        # Verify SCD2 metadata
        assert "IsActive" in scd2_result.columns
        assert "StartDate" in scd2_result.columns
        assert "EndDate" in scd2_result.columns
        assert "OpTs" in scd2_result.columns
        
        # All should be active
        active_count = scd2_result.filter(scd2_result.IsActive == True).count()
        assert active_count == scd2_result.count()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])