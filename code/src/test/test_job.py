"""
Unit tests for AWS Glue ETL Job

Tests validate transformation logic without requiring AWS infrastructure.
All external dependencies (S3, Glue Catalog, Hudi) are mocked.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def mock_glue_context():
    """Mock Glue context"""
    mock_context = Mock()
    mock_context.get_logger = Mock(return_value=Mock())
    return mock_context


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
        ("O001", "Widget", "10.50", "2", "2024-01-01", "C001"),  # Duplicate
        ("O004", "Null", "20.00", "1", "2024-01-04", "C004")  # Contains "Null"
    ]
    
    return spark.createDataFrame(data, schema)


class TestDataCleaner:
    """Test DataCleaner class"""
    
    def test_remove_nulls_string_null(self, spark, sample_customer_data):
        """Test removal of string 'Null' values"""
        from src.main.job import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.remove_nulls(sample_customer_data, "Null")
        
        # Should remove record with "Null" in Name and record with NULL
        assert result.count() == 4
        
        # Verify no "Null" strings remain
        null_records = result.filter(result.Name == "Null")
        assert null_records.count() == 0
    
    def test_remove_nulls_actual_null(self, spark, sample_customer_data):
        """Test removal of actual NULL values"""
        from src.main.job import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.remove_nulls(sample_customer_data, "Null")
        
        # Verify no NULL values remain
        null_records = result.filter(result.Name.isNull())
        assert null_records.count() == 0
    
    def test_remove_duplicates(self, spark, sample_customer_data):
        """Test duplicate removal"""
        from src.main.job import DataCleaner
        
        cleaner = DataCleaner()
        # First remove nulls to get clean data
        no_nulls = cleaner.remove_nulls(sample_customer_data, "Null")
        result = cleaner.remove_duplicates(no_nulls)
        
        # Should have 3 unique records (C001, C002, C003)
        assert result.count() == 3
        
        # Verify C001 appears only once
        c001_records = result.filter(result.CustId == "C001")
        assert c001_records.count() == 1


class TestS3Reader:
    """Test S3Reader class"""
    
    def test_read_csv(self, mock_glue_context, spark, sample_customer_data):
        """Test CSV reading from S3"""
        from src.main.job import S3Reader
        
        # Mock the Glue context's create_dynamic_frame method
        mock_dynamic_frame = Mock()
        mock_dynamic_frame.toDF.return_value = sample_customer_data
        mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dynamic_frame
        
        reader = S3Reader(mock_glue_context)
        result = reader.read_csv("s3://test-bucket/data/", separator=",", with_header=True)
        
        # Verify DataFrame is returned
        assert result is not None
        assert result.count() == 6


class TestS3Writer:
    """Test S3Writer class"""
    
    def test_write_parquet(self, spark, sample_customer_data):
        """Test Parquet write operation"""
        from src.main.job import S3Writer
        
        writer = S3Writer(spark)
        
        # Mock the _write_parquet method
        with patch.object(writer, '_write_parquet') as mock_write:
            writer.write_dataframe(sample_customer_data, "s3://test/path", format_type="parquet")
            mock_write.assert_called_once()
    
    def test_write_hudi(self, spark, sample_customer_data):
        """Test Hudi write operation"""
        from src.main.job import S3Writer
        
        writer = S3Writer(spark)
        hudi_options = {'hoodie.table.name': 'test_table'}
        
        # Mock the _write_hudi method
        with patch.object(writer, '_write_hudi') as mock_write:
            writer.write_dataframe(
                sample_customer_data,
                "s3://test/path",
                format_type="hudi",
                hudi_options=hudi_options
            )
            mock_write.assert_called_once()


class TestHudiManager:
    """Test HudiManager class"""
    
    def test_apply_scd2_logic_initial_load(self, spark, sample_customer_data):
        """Test SCD2 logic for initial load (no existing data)"""
        from src.main.job import HudiManager
        
        manager = HudiManager(spark)
        result = manager.apply_scd2_logic(sample_customer_data, None, ["CustId"])
        
        # Verify SCD2 metadata columns are added
        assert "IsActive" in result.columns
        assert "StartDate" in result.columns
        assert "EndDate" in result.columns
        assert "OpTs" in result.columns
        
        # Verify all records are active
        active_count = result.filter(result.IsActive == True).count()
        assert active_count == result.count()
    
    def test_apply_scd2_logic_incremental(self, spark, sample_customer_data):
        """Test SCD2 logic for incremental load"""
        from src.main.job import HudiManager
        
        manager = HudiManager(spark)
        
        # Create mock existing data
        existing_df = sample_customer_data.limit(2)
        
        result = manager.apply_scd2_logic(sample_customer_data, existing_df, ["CustId"])
        
        # Verify metadata columns exist
        assert "IsActive" in result.columns
        assert "OpTs" in result.columns


class TestCustomerOrderETL:
    """Test CustomerOrderETL class"""
    
    def test_clean_customer_data(self, mock_glue_context, spark, sample_customer_data):
        """Test customer data cleaning pipeline"""
        from src.main.job import CustomerOrderETL
        
        params = {
            'null_string_value': 'Null',
            'catalog_database': 'test_db'
        }
        
        etl = CustomerOrderETL(mock_glue_context, spark, params)
        result = etl.clean_customer_data(sample_customer_data)
        
        # Should have 3 clean records (removed duplicates and nulls)
        assert result.count() == 3
        
        # Verify no nulls or "Null" strings
        null_records = result.filter(
            (result.Name == "Null") | (result.Name.isNull())
        )
        assert null_records.count() == 0
    
    def test_clean_order_data(self, mock_glue_context, spark, sample_order_data):
        """Test order data cleaning pipeline"""
        from src.main.job import CustomerOrderETL
        
        params = {
            'null_string_value': 'Null',
            'catalog_database': 'test_db'
        }
        
        etl = CustomerOrderETL(mock_glue_context, spark, params)
        result = etl.clean_order_data(sample_order_data)
        
        # Should have 3 clean records
        assert result.count() == 3
        
        # Verify numeric columns are properly typed
        assert dict(result.dtypes)['PricePerUnit'] == 'double'
        assert dict(result.dtypes)['Qty'] == 'int'
    
    def test_log_record_count(self, mock_glue_context, spark, sample_customer_data):
        """Test record count logging"""
        from src.main.job import CustomerOrderETL
        
        params = {'catalog_database': 'test_db'}
        etl = CustomerOrderETL(mock_glue_context, spark, params)
        
        count = etl.log_record_count(sample_customer_data, "Test stage")
        
        assert count == 6
        etl.logger.info.assert_called()


class TestIntegration:
    """Integration tests for complete pipeline"""
    
    def test_end_to_end_cleaning(self, mock_glue_context, spark, sample_customer_data, sample_order_data):
        """Test end-to-end data cleaning"""
        from src.main.job import CustomerOrderETL
        
        params = {
            'null_string_value': 'Null',
            'catalog_database': 'test_db',
            'customer_source_path': 's3://test/customer/',
            'order_source_path': 's3://test/order/',
            'csv_separator': ',',
            'csv_header': 'True'
        }
        
        etl = CustomerOrderETL(mock_glue_context, spark, params)
        
        # Clean both datasets
        customer_clean = etl.clean_customer_data(sample_customer_data)
        order_clean = etl.clean_order_data(sample_order_data)
        
        # Verify cleaning results
        assert customer_clean.count() == 3
        assert order_clean.count() == 3
        
        # Verify join would work
        joined = customer_clean.join(order_clean, "CustId", "inner")
        assert joined.count() == 3
    
    def test_aggregation_calculation(self, spark):
        """Test customer aggregate spend calculation"""
        from pyspark.sql.functions import sum as _sum
        
        # Create sample order summary data
        schema = StructType([
            StructField("Name", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("IsActive", BooleanType(), True)
        ])
        
        data = [
            ("John Doe", 10.50, 2, "2024-01-01", True),
            ("John Doe", 15.00, 1, "2024-01-01", True),
            ("Jane Smith", 25.00, 1, "2024-01-02", True),
            ("John Doe", 10.00, 1, "2024-01-01", False)  # Inactive record
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Filter active and calculate aggregate
        active_df = df.filter(df.IsActive == True)
        with_total = active_df.withColumn("TotalAmount", active_df.PricePerUnit * active_df.Qty)
        agg_df = with_total.groupBy("Name", "Date").agg(_sum("TotalAmount").alias("TotalAmount"))
        
        # Verify aggregation
        assert agg_df.count() == 2
        
        # Verify John Doe's total for 2024-01-01 is 36.00 (10.50*2 + 15.00*1)
        john_total = agg_df.filter(
            (agg_df.Name == "John Doe") & (agg_df.Date == "2024-01-01")
        ).collect()[0]["TotalAmount"]
        assert john_total == 36.00


def test_get_job_parameters():
    """Test job parameter retrieval with defaults"""
    from src.main.job import get_job_parameters
    
    params = get_job_parameters()
    
    # Verify default parameters are set
    assert params['catalog_database'] == 'gen_ai_poc_databrickscoe'
    assert params['null_string_value'] == 'Null'
    assert params['hudi_table_type'] == 'COPY_ON_WRITE'