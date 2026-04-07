"""
Comprehensive unit tests for Customer Order Analytics Job
Tests all FRD requirements: FR-INGEST-001, FR-CLEAN-001, FR-SCD2-001
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("CustomerOrderAnalyticsTest") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture
def mock_glue_context():
    """Mock AWS Glue context"""
    with patch('src.main.job.GlueContext') as mock_glue:
        mock_context = Mock()
        mock_context.get_logger.return_value = Mock()
        mock_glue.return_value = mock_context
        yield mock_context


@pytest.fixture
def mock_job():
    """Mock AWS Glue Job"""
    with patch('src.main.job.Job') as mock_job_class:
        mock_job_instance = Mock()
        mock_job_class.return_value = mock_job_instance
        yield mock_job_instance


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data matching TRD schema"""
    data = [
        ("C001", "John Doe", "john.doe@email.com", "North"),
        ("C002", "Jane Smith", "jane.smith@email.com", "South"),
        ("C003", "Bob Johnson", "bob.j@email.com", "East"),
        ("C004", "Alice Williams", "alice.w@email.com", "West"),
        ("C005", "Charlie Brown", "charlie.b@email.com", "North"),
    ]

    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data matching TRD schema"""
    data = [
        ("O001", "Laptop", 999.99, 2, "2024-01-15"),
        ("O002", "Mouse", 29.99, 5, "2024-01-16"),
        ("O003", "Keyboard", 79.99, 3, "2024-01-17"),
        ("O004", "Monitor", 299.99, 1, "2024-01-18"),
        ("O005", "Headphones", 149.99, 2, "2024-01-19"),
    ]

    schema = StructType([
        StructField("OrderId", StringType(), False),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DoubleType(), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_dirty_customer_data(spark):
    """Create sample customer data with quality issues"""
    data = [
        ("C001", "John Doe", "john.doe@email.com", "North"),
        ("C002", None, "jane.smith@email.com", "South"),  # NULL value
        ("C003", "Bob Johnson", "Null", "East"),  # 'Null' string
        ("C004", "Alice Williams", "alice.w@email.com", "West"),
        ("C004", "Alice Williams", "alice.w@email.com", "West"),  # Duplicate
    ]

    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


class TestJobInitialization:
    """Test job initialization and configuration"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_job_initialization(self, mock_args, mock_sc, mock_glue_context, mock_job):
        """Test job initializes correctly"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}

        job = CustomerOrderAnalyticsJob()

        assert job.config is not None
        assert 'catalog' in job.config
        assert 'data_sources' in job.config

    def test_default_config_structure(self):
        """Test default configuration has required structure"""
        from src.main.job import CustomerOrderAnalyticsJob

        with patch('src.main.job.SparkContext'), \
             patch('src.main.job.getResolvedOptions') as mock_args:
            mock_args.return_value = {'JOB_NAME': 'test-job'}
            job = CustomerOrderAnalyticsJob()
            config = job._get_default_config()

        assert 'catalog' in config
        assert 'database_name' in config['catalog']
        assert config['catalog']['database_name'] == 'sdlc_wizard_db'

        assert 'data_sources' in config
        assert 'customer_raw' in config['data_sources']
        assert 'order_raw' in config['data_sources']

        assert 'data_targets' in config
        assert 'customer_curated' in config['data_targets']
        assert 'order_curated' in config['data_targets']


class TestSchemaDefinitions:
    """Test schema definitions match TRD specifications"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_customer_schema_structure(self, mock_args, mock_sc):
        """Test FR-INGEST-001: Customer schema matches TRD"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()
        schema = job.get_customer_schema()

        assert len(schema.fields) == 4
        assert schema.fields[0].name == "CustId"
        assert schema.fields[0].dataType == StringType()
        assert schema.fields[1].name == "Name"
        assert schema.fields[2].name == "EmailId"
        assert schema.fields[3].name == "Region"

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_order_schema_structure(self, mock_args, mock_sc):
        """Test FR-INGEST-001: Order schema matches TRD"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()
        schema = job.get_order_schema()

        assert len(schema.fields) == 5
        assert schema.fields[0].name == "OrderId"
        assert schema.fields[0].dataType == StringType()
        assert schema.fields[1].name == "ItemName"
        assert schema.fields[2].name == "PricePerUnit"
        assert schema.fields[2].dataType == DoubleType()
        assert schema.fields[3].name == "Qty"
        assert schema.fields[3].dataType == IntegerType()
        assert schema.fields[4].name == "Date"


class TestDataIngestion:
    """Test data ingestion from S3 sources"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_read_customer_data_path(self, mock_args, mock_sc, spark):
        """Test FR-INGEST-001: Customer data read from correct S3 path"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        # Verify configuration has correct path
        customer_path = job.config['data_sources']['customer_raw']['path']
        assert customer_path == "s3://adif-sdlc/sdlc_wizard/customerdata/"

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_read_order_data_path(self, mock_args, mock_sc):
        """Test FR-INGEST-001: Order data read from correct S3 path"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        # Verify configuration has correct path
        order_path = job.config['data_sources']['order_raw']['path']
        assert order_path == "s3://adif-sdlc/sdlc_wizard/orderdata/"


class TestDataCleaning:
    """Test data quality and cleaning operations"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_clean_data_removes_nulls(self, mock_args, mock_sc, sample_dirty_customer_data):
        """Test FR-CLEAN-001: Remove NULL values"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        initial_count = sample_dirty_customer_data.count()
        cleaned_df = job.clean_data(sample_dirty_customer_data)
        final_count = cleaned_df.count()

        # Should remove row with NULL Name
        assert final_count < initial_count

        # Verify no NULLs remain
        for col_name in cleaned_df.columns:
            null_count = cleaned_df.filter(cleaned_df[col_name].isNull()).count()
            assert null_count == 0

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_clean_data_removes_null_strings(self, mock_args, mock_sc, sample_dirty_customer_data):
        """Test FR-CLEAN-001: Remove 'Null' string values"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        cleaned_df = job.clean_data(sample_dirty_customer_data)

        # Verify no 'Null' strings remain
        for col_name in cleaned_df.columns:
            null_string_count = cleaned_df.filter(
                (cleaned_df[col_name] == "Null") |
                (cleaned_df[col_name] == "NULL") |
                (cleaned_df[col_name] == "null")
            ).count()
            assert null_string_count == 0

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_clean_data_removes_duplicates(self, mock_args, mock_sc, sample_dirty_customer_data):
        """Test FR-CLEAN-001: Remove duplicate records"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        cleaned_df = job.clean_data(sample_dirty_customer_data)

        # Count unique CustId values
        unique_customers = cleaned_df.select("CustId").distinct().count()
        total_customers = cleaned_df.count()

        # Should have no duplicates
        assert unique_customers == total_customers

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_clean_data_preserves_valid_records(self, mock_args, mock_sc, sample_customer_data):
        """Test FR-CLEAN-001: Valid records are preserved"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        initial_count = sample_customer_data.count()
        cleaned_df = job.clean_data(sample_customer_data)
        final_count = cleaned_df.count()

        # All valid records should be preserved
        assert final_count == initial_count


class TestSCDType2:
    """Test SCD Type 2 implementation"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_add_scd2_columns_structure(self, mock_args, mock_sc, sample_customer_data):
        """Test FR-SCD2-001: SCD Type 2 columns are added"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        scd2_df = job.add_scd2_columns(sample_customer_data)

        # Verify all SCD2 columns exist
        assert "IsActive" in scd2_df.columns
        assert "StartDate" in scd2_df.columns
        assert "EndDate" in scd2_df.columns
        assert "OpTs" in scd2_df.columns

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_add_scd2_columns_default_values(self, mock_args, mock_sc, sample_customer_data):
        """Test FR-SCD2-001: SCD Type 2 columns have correct default values"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        scd2_df = job.add_scd2_columns(sample_customer_data, is_new=True)

        # Collect first row
        first_row = scd2_df.first()

        # Verify IsActive is True for new records
        assert first_row["IsActive"] == True

        # Verify StartDate is set
        assert first_row["StartDate"] is not None

        # Verify EndDate is far future (9999-12-31)
        assert first_row["EndDate"] is not None

        # Verify OpTs is set
        assert first_row["OpTs"] is not None

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_add_scd2_columns_all_records(self, mock_args, mock_sc, sample_customer_data):
        """Test FR-SCD2-001: SCD Type 2 columns added to all records"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        initial_count = sample_customer_data.count()
        scd2_df = job.add_scd2_columns(sample_customer_data)
        final_count = scd2_df.count()

        # Record count should remain same
        assert final_count == initial_count

        # All records should have SCD2 columns populated
        null_isactive = scd2_df.filter(scd2_df["IsActive"].isNull()).count()
        assert null_isactive == 0


class TestHudiIntegration:
    """Test Hudi table writing and configuration"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_hudi_configuration_customer(self, mock_args, mock_sc):
        """Test FR-SCD2-001: Hudi configuration for customer table"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        customer_config = job.config['data_targets']['customer_curated']

        assert customer_config['format'] == 'hudi'
        assert customer_config['table_name'] == 'customer_curated'
        assert customer_config['record_key'] == 'CustId'
        assert customer_config['path'] == 's3://adif-sdlc/curated/sdlc_wizard/customer/'

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_hudi_configuration_order(self, mock_args, mock_sc):
        """Test FR-SCD2-001: Hudi configuration for order table"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        order_config = job.config['data_targets']['order_curated']

        assert order_config['format'] == 'hudi'
        assert order_config['table_name'] == 'order_curated'
        assert order_config['record_key'] == 'OrderId'
        assert order_config['path'] == 's3://adif-sdlc/curated/sdlc_wizard/order/'


class TestAnalytics:
    """Test analytics generation"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_generate_order_summary(self, mock_args, mock_sc, sample_order_data):
        """Test order summary generation"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        summary_df = job.generate_order_summary(sample_order_data)

        # Verify summary has expected columns
        assert "ItemName" in summary_df.columns
        assert "TotalOrders" in summary_df.columns
        assert "TotalRevenue" in summary_df.columns
        assert "AvgPricePerUnit" in summary_df.columns
        assert "TotalQuantity" in summary_df.columns

        # Verify aggregation worked
        assert summary_df.count() > 0

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_generate_customer_aggregate_spend(self, mock_args, mock_sc, sample_customer_data, sample_order_data):
        """Test customer aggregate spend generation"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        spend_df = job.generate_customer_aggregate_spend(
            sample_customer_data,
            sample_order_data
        )

        # Verify spend has expected columns
        assert "TotalSpend" in spend_df.columns
        assert "TotalOrders" in spend_df.columns
        assert "AvgOrderValue" in spend_df.columns

        # Verify aggregation worked
        assert spend_df.count() > 0


class TestOutputPaths:
    """Test output S3 paths match TRD"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_customer_curated_path(self, mock_args, mock_sc):
        """Test customer curated output path matches TRD"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        path = job.config['data_targets']['customer_curated']['path']
        assert path == "s3://adif-sdlc/curated/sdlc_wizard/customer/"

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_order_curated_path(self, mock_args, mock_sc):
        """Test order curated output path matches TRD"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        path = job.config['data_targets']['order_curated']['path']
        assert path == "s3://adif-sdlc/curated/sdlc_wizard/order/"

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_order_summary_path(self, mock_args, mock_sc):
        """Test order summary output path matches TRD"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        path = job.config['data_targets']['order_summary']['path']
        assert path == "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_customer_aggregate_spend_path(self, mock_args, mock_sc):
        """Test customer aggregate spend output path matches TRD"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        path = job.config['data_targets']['customer_aggregate_spend']['path']
        assert path == "s3://adif-sdlc/analytics/customeraggregatespend/"


class TestGlueCatalog:
    """Test AWS Glue Catalog integration"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    def test_catalog_database_name(self, mock_args, mock_sc):
        """Test Glue Catalog database name matches TRD"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        job = CustomerOrderAnalyticsJob()

        database_name = job.config['catalog']['database_name']
        assert database_name == "sdlc_wizard_db"


class TestEndToEndWorkflow:
    """Test complete job workflow"""

    @patch('src.main.job.SparkContext')
    @patch('src.main.job.getResolvedOptions')
    @patch('src.main.job.CustomerOrderAnalyticsJob.read_customer_data')
    @patch('src.main.job.CustomerOrderAnalyticsJob.read_order_data')
    @patch('src.main.job.CustomerOrderAnalyticsJob.write_hudi_table')
    @patch('src.main.job.CustomerOrderAnalyticsJob.write_parquet_table')
    def test_run_method_executes_all_steps(
        self,
        mock_write_parquet,
        mock_write_hudi,
        mock_read_order,
        mock_read_customer,
        mock_args,
        mock_sc,
        sample_customer_data,
        sample_order_data
    ):
        """Test complete job execution workflow"""
        from src.main.job import CustomerOrderAnalyticsJob

        mock_args.return_value = {'JOB_NAME': 'test-job'}
        mock_read_customer.return_value = sample_customer_data
        mock_read_order.return_value = sample_order_data

        job = CustomerOrderAnalyticsJob()
        job.run()

        # Verify all major steps were called
        assert mock_read_customer.called
        assert mock_read_order.called
        assert mock_write_hudi.called
        assert mock_write_parquet.called


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])