"""
Unit tests for AWS Glue ETL Job - Use Case 1
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from main.job import (
    DataCleaner, SCD2Processor, AggregationProcessor,
    S3Reader, S3Writer
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test_glue_job") \
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
        ("C003", "Bob Wilson", "Null", "East"),
        ("C004", None, "alice@example.com", "West"),
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data"""
    schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])
    
    data = [
        ("O001", "Widget", 10.0, 2, "2024-01-01", "C001"),
        ("O002", "Gadget", 20.0, 1, "2024-01-02", "C002"),
        ("O003", "Tool", 15.0, 3, "2024-01-03", "C001"),
        ("O004", "Null", 25.0, 1, "2024-01-04", "C003"),
        ("O005", "Device", None, 2, "2024-01-05", "C004")
    ]
    
    return spark.createDataFrame(data, schema)


class TestDataCleaner:
    """Test DataCleaner class"""
    
    def test_remove_nulls_string_null(self, sample_customer_data):
        """Test removal of string 'Null' values"""
        cleaned = DataCleaner.remove_nulls(sample_customer_data, "Null")
        
        # Should remove row with "Null" in EmailId
        assert cleaned.count() == 3
        
        # Verify no "Null" strings remain
        null_count = cleaned.filter(cleaned.EmailId == "Null").count()
        assert null_count == 0
    
    def test_remove_nulls_actual_null(self, sample_customer_data):
        """Test removal of actual NULL values"""
        cleaned = DataCleaner.remove_nulls(sample_customer_data, "Null")
        
        # Should remove rows with NULL values
        assert cleaned.count() == 3
        
        # Verify no NULL values remain
        for column in cleaned.columns:
            null_count = cleaned.filter(cleaned[column].isNull()).count()
            assert null_count == 0
    
    def test_remove_duplicates(self, sample_customer_data):
        """Test duplicate removal"""
        # First remove nulls to get clean data
        cleaned = DataCleaner.remove_nulls(sample_customer_data, "Null")
        deduped = DataCleaner.remove_duplicates(cleaned)
        
        # Should have 2 unique records after removing nulls and duplicates
        assert deduped.count() == 2


class TestSCD2Processor:
    """Test SCD2Processor class"""
    
    def test_add_scd2_columns(self, sample_customer_data):
        """Test addition of SCD Type 2 columns"""
        result = SCD2Processor.add_scd2_columns(sample_customer_data, is_active=True)
        
        # Verify new columns exist
        assert "IsActive" in result.columns
        assert "StartDate" in result.columns
        assert "EndDate" in result.columns
        assert "OpTs" in result.columns
        
        # Verify IsActive is True
        active_count = result.filter(result.IsActive == True).count()
        assert active_count == result.count()
        
        # Verify EndDate is NULL for active records
        null_end_date = result.filter(result.EndDate.isNull()).count()
        assert null_end_date == result.count()
    
    def test_join_customer_order(self, spark):
        """Test customer-order join"""
        # Create clean test data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South")
        ]
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        customer_df = spark.createDataFrame(customer_data, customer_schema)
        
        order_data = [
            ("O001", "Widget", 10.0, 2, "2024-01-01", "C001"),
            ("O002", "Gadget", 20.0, 1, "2024-01-02", "C002"),
            ("O003", "Tool", 15.0, 3, "2024-01-03", "C001")
        ]
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("CustId", StringType(), True)
        ])
        order_df = spark.createDataFrame(order_data, order_schema)
        
        joined = SCD2Processor.join_customer_order(customer_df, order_df)
        
        # Should have 3 joined records
        assert joined.count() == 3
        
        # Verify all customer and order columns present
        assert "Name" in joined.columns
        assert "OrderId" in joined.columns
        assert "CustId" in joined.columns


class TestAggregationProcessor:
    """Test AggregationProcessor class"""
    
    def test_calculate_customer_spend(self, spark):
        """Test customer spend aggregation"""
        # Create clean test data
        customer_data = [
            ("C001", "John Doe", "john@example.com", "North"),
            ("C002", "Jane Smith", "jane@example.com", "South")
        ]
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        customer_df = spark.createDataFrame(customer_data, customer_schema)
        
        order_data = [
            ("O001", "Widget", 10.0, 2, "2024-01-01", "C001"),  # 20.0
            ("O002", "Gadget", 20.0, 1, "2024-01-02", "C002"),  # 20.0
            ("O003", "Tool", 15.0, 3, "2024-01-01", "C001")     # 45.0
        ]
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("CustId", StringType(), True)
        ])
        order_df = spark.createDataFrame(order_data, order_schema)
        
        agg_df = AggregationProcessor.calculate_customer_spend(customer_df, order_df)
        
        # Should have 2 aggregate records (John on 2024-01-01, Jane on 2024-01-02)
        assert agg_df.count() == 2
        
        # Verify columns
        assert "Name" in agg_df.columns
        assert "TotalAmount" in agg_df.columns
        assert "Date" in agg_df.columns
        
        # Verify John's total for 2024-01-01 is 65.0 (20 + 45)
        john_total = agg_df.filter(
            (agg_df.Name == "John Doe") & (agg_df.Date == "2024-01-01")
        ).select("TotalAmount").collect()[0][0]
        assert john_total == 65.0


class TestS3Writer:
    """Test S3Writer class"""
    
    def test_write_parquet(self, spark, tmp_path):
        """Test Parquet write functionality"""
        mock_glue_context = Mock()
        writer = S3Writer(spark, mock_glue_context)
        
        # Create test data
        data = [("C001", "John Doe"), ("C002", "Jane Smith")]
        schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Write to temp path
        output_path = str(tmp_path / "test_output")
        writer._write_parquet(df, output_path, mode="overwrite")
        
        # Verify file was written
        assert os.path.exists(output_path)
        
        # Read back and verify
        result_df = spark.read.parquet(output_path)
        assert result_df.count() == 2
    
    def test_write_dataframe_dispatch(self, spark, tmp_path):
        """Test write_dataframe format dispatch"""
        mock_glue_context = Mock()
        writer = S3Writer(spark, mock_glue_context)
        
        data = [("C001", "John Doe")]
        schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        output_path = str(tmp_path / "test_dispatch")
        
        # Test parquet dispatch
        writer.write_dataframe(df, output_path, format_type="parquet")
        assert os.path.exists(output_path)
        
        # Test invalid format
        with pytest.raises(ValueError, match="Unsupported format"):
            writer.write_dataframe(df, output_path, format_type="invalid")


class TestS3Reader:
    """Test S3Reader class"""
    
    def test_read_csv(self, spark, tmp_path):
        """Test CSV read functionality"""
        mock_glue_context = Mock()
        reader = S3Reader(spark, mock_glue_context)
        
        # Create test CSV file
        csv_path = tmp_path / "test.csv"
        csv_content = "CustId,Name\nC001,John Doe\nC002,Jane Smith"
        csv_path.write_text(csv_content)
        
        # Read CSV
        df = reader.read_csv(str(tmp_path / "*.csv"), header=True, infer_schema=True)
        
        # Verify data
        assert df.count() == 2
        assert "CustId" in df.columns
        assert "Name" in df.columns


class TestIntegration:
    """Integration tests for complete workflow"""
    
    def test_end_to_end_cleaning_workflow(self, sample_customer_data, sample_order_data):
        """Test complete data cleaning workflow"""
        # Clean customer data
        customer_clean = DataCleaner.remove_nulls(sample_customer_data, "Null")
        customer_clean = DataCleaner.remove_duplicates(customer_clean)
        
        # Clean order data
        order_clean = DataCleaner.remove_nulls(sample_order_data, "Null")
        order_clean = DataCleaner.remove_duplicates(order_clean)
        
        # Verify cleaning results
        assert customer_clean.count() == 2  # 2 valid unique customers
        assert order_clean.count() == 2     # 2 valid unique orders
        
        # Verify no nulls remain
        for column in customer_clean.columns:
            assert customer_clean.filter(customer_clean[column].isNull()).count() == 0
        
        for column in order_clean.columns:
            assert order_clean.filter(order_clean[column].isNull()).count() == 0
    
    def test_scd2_workflow(self, spark):
        """Test SCD Type 2 workflow"""
        # Create clean test data
        customer_data = [("C001", "John Doe", "john@example.com", "North")]
        customer_schema = StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        customer_df = spark.createDataFrame(customer_data, customer_schema)
        
        order_data = [("O001", "Widget", 10.0, 2, "2024-01-01", "C001")]
        order_schema = StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("CustId", StringType(), True)
        ])
        order_df = spark.createDataFrame(order_data, order_schema)
        
        # Join and add SCD2 columns
        joined = SCD2Processor.join_customer_order(customer_df, order_df)
        scd2_df = SCD2Processor.add_scd2_columns(joined, is_active=True)
        
        # Verify SCD2 structure
        assert scd2_df.count() == 1
        assert "IsActive" in scd2_df.columns
        assert "StartDate" in scd2_df.columns
        assert "EndDate" in scd2_df.columns
        assert "OpTs" in scd2_df.columns
        
        # Verify active record
        active_records = scd2_df.filter(scd2_df.IsActive == True)
        assert active_records.count() == 1
        
        # Verify EndDate is NULL for active record
        null_end_dates = scd2_df.filter(scd2_df.EndDate.isNull())
        assert null_end_dates.count() == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])