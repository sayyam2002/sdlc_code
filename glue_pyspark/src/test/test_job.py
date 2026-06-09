"""
Unit tests for Customer Order Pipeline ETL Job
"""

import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# Initialize Spark for testing
def get_spark():
    """Get or create Spark session for testing"""
    if hasattr(sys.modules['builtins'], 'spark'):
        return getattr(sys.modules['builtins'], 'spark')

    spark = SparkSession.builder \
        .appName("test_customer_order_pipeline") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = get_spark()


def test_config_manager_loads_parameters():
    """Test ConfigManager loads parameters correctly"""
    from main.job import ConfigManager

    params = ConfigManager.get_job_parameters()

    assert params is not None
    assert isinstance(params, dict)
    assert 'inputs_source_customer_path' in params
    assert 'inputs_source_order_path' in params
    assert params['inputs_source_customer_path'] == 's3://adif-sdlc/sdlc_wizard/customerdata/'
    assert params['inputs_source_order_path'] == 's3://adif-sdlc/sdlc_wizard/orderdata/'


def test_s3_data_reader_reads_customer_data():
    """Test S3DataReader reads and validates customer data"""
    from main.job import S3DataReader

    logger = Mock()
    reader = S3DataReader(spark, logger)

    # Create sample customer data
    customer_data = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', 'Jane Smith', 'jane@example.com', 'South'),
        ('C003', 'Bob Johnson', 'bob@example.com', 'East')
    ]

    sample_df = spark.createDataFrame(
        customer_data,
        ['CustId', 'Name', 'EmailId', 'Region']
    )

    # Mock the internal read method
    with patch.object(reader, '_read_csv_internal', return_value=sample_df):
        result_df = reader.read_data_safe(
            path='s3://test-bucket/customer/',
            expected_schema=['CustId', 'Name', 'EmailId', 'Region'],
            data_type='customer'
        )

    assert result_df is not None
    assert result_df.count() == 3
    assert set(result_df.columns) == {'custid', 'name', 'emailid', 'region'}

    # Verify logging
    assert logger.info.called
    assert any('Successfully loaded' in str(call) for call in logger.info.call_args_list)


def test_s3_data_reader_reads_order_data():
    """Test S3DataReader reads and validates order data"""
    from main.job import S3DataReader

    logger = Mock()
    reader = S3DataReader(spark, logger)

    # Create sample order data
    order_data = [
        ('O001', 'Laptop', 1200.00, 1, '2024-01-15', 'C001'),
        ('O002', 'Mouse', 25.50, 2, '2024-01-16', 'C002'),
        ('O003', 'Keyboard', 75.00, 1, '2024-01-17', 'C003')
    ]

    sample_df = spark.createDataFrame(
        order_data,
        ['OrderId', 'ItemName', 'PricePerUnit', 'Qty', 'Date', 'CustId']
    )

    # Mock the internal read method
    with patch.object(reader, '_read_csv_internal', return_value=sample_df):
        result_df = reader.read_data_safe(
            path='s3://test-bucket/order/',
            expected_schema=['OrderId', 'ItemName', 'PricePerUnit', 'Qty', 'Date', 'CustId'],
            data_type='order'
        )

    assert result_df is not None
    assert result_df.count() == 3
    assert set(result_df.columns) == {'orderid', 'itemname', 'priceperunit', 'qty', 'date', 'custid'}


def test_s3_data_reader_validates_schema():
    """Test S3DataReader validates schema correctly"""
    from main.job import S3DataReader

    logger = Mock()
    reader = S3DataReader(spark, logger)

    # Create data with wrong schema
    wrong_data = [
        ('C001', 'John Doe', 'North'),  # Missing EmailId column
    ]

    sample_df = spark.createDataFrame(
        wrong_data,
        ['CustId', 'Name', 'Region']
    )

    # Mock the internal read method
    with patch.object(reader, '_read_csv_internal', return_value=sample_df):
        try:
            reader.read_data_safe(
                path='s3://test-bucket/customer/',
                expected_schema=['CustId', 'Name', 'EmailId', 'Region'],
                data_type='customer'
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert 'Schema validation failed' in str(e)
            assert logger.error.called


def test_data_quality_transformer_removes_nulls():
    """Test DataQualityTransformer removes NULL values"""
    from main.job import DataQualityTransformer

    logger = Mock()
    transformer = DataQualityTransformer(logger)

    # Create data with NULLs
    data_with_nulls = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', None, 'jane@example.com', 'South'),
        ('C003', 'Bob Johnson', 'Null', 'East'),
        (None, 'Alice Brown', 'alice@example.com', 'West')
    ]

    df = spark.createDataFrame(
        data_with_nulls,
        ['custid', 'name', 'emailid', 'region']
    )

    result_df = transformer.remove_nulls(df, 'customer')

    assert result_df.count() == 1  # Only first row should remain
    assert result_df.first()['custid'] == 'C001'
    assert logger.info.called


def test_data_quality_transformer_removes_duplicates():
    """Test DataQualityTransformer removes duplicate records"""
    from main.job import DataQualityTransformer

    logger = Mock()
    transformer = DataQualityTransformer(logger)

    # Create data with duplicates
    data_with_dupes = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', 'Jane Smith', 'jane@example.com', 'South'),
        ('C001', 'John Doe', 'john@example.com', 'North'),  # Duplicate
        ('C003', 'Bob Johnson', 'bob@example.com', 'East')
    ]

    df = spark.createDataFrame(
        data_with_dupes,
        ['custid', 'name', 'emailid', 'region']
    )

    result_df = transformer.remove_duplicates(df, 'customer')

    assert result_df.count() == 3  # Should have 3 unique records
    assert logger.info.called


def test_s3_data_writer_writes_parquet():
    """Test S3DataWriter writes data in Parquet format"""
    from main.job import S3DataWriter

    logger = Mock()
    glueContext = Mock()
    writer = S3DataWriter(spark, glueContext, logger)

    # Create sample data
    data = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', 'Jane Smith', 'jane@example.com', 'South')
    ]

    df = spark.createDataFrame(
        data,
        ['custid', 'name', 'emailid', 'region']
    )

    # Mock the internal write methods
    with patch.object(writer, '_write_parquet') as mock_write:
        writer.write_data_safe(
            df=df,
            path='s3://test-bucket/customer/',
            table_name='test_customer',
            database_name='test_db',
            data_type='customer'
        )

        assert mock_write.called
        assert logger.info.called


def test_s3_data_writer_writes_hudi_scd2():
    """Test S3DataWriter writes Hudi format with SCD Type 2 columns"""
    from main.job import S3DataWriter

    logger = Mock()
    glueContext = Mock()
    writer = S3DataWriter(spark, glueContext, logger)

    # Create sample order summary data
    data = [
        ('O001', 'Laptop', 1200.00, 1, '2024-01-15', 'C001', 'John Doe', 'john@example.com', 'North', 1200.00)
    ]

    df = spark.createDataFrame(
        data,
        ['orderid', 'itemname', 'priceperunit', 'qty', 'date', 'custid', 'name', 'emailid', 'region', 'totalamount']
    )

    hudi_config = {
        'hudi_table_name': 'ordersummary',
        'hudi_record_key': 'orderid',
        'hudi_precombine_field': 'OpTs',
        'hudi_partition_field': 'date',
        'hudi_operation': 'upsert'
    }

    # Mock the internal write method
    with patch.object(writer, '_write_hudi') as mock_write:
        writer.write_hudi_scd2(
            df=df,
            path='s3://test-bucket/ordersummary/',
            hudi_config=hudi_config,
            data_type='order_summary'
        )

        assert mock_write.called

        # Verify SCD Type 2 columns were added
        call_args = mock_write.call_args
        df_with_scd = call_args[0][0]
        assert 'IsActive' in df_with_scd.columns
        assert 'StartDate' in df_with_scd.columns
        assert 'EndDate' in df_with_scd.columns
        assert 'OpTs' in df_with_scd.columns


def test_order_summary_processor_creates_summary():
    """Test OrderSummaryProcessor creates order summary correctly"""
    from main.job import OrderSummaryProcessor

    logger = Mock()
    processor = OrderSummaryProcessor(logger)

    # Create sample customer data
    customer_data = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', 'Jane Smith', 'jane@example.com', 'South')
    ]

    customer_df = spark.createDataFrame(
        customer_data,
        ['custid', 'name', 'emailid', 'region']
    )

    # Create sample order data
    order_data = [
        ('O001', 'Laptop', 1200.00, 1, '2024-01-15', 'C001'),
        ('O002', 'Mouse', 25.50, 2, '2024-01-16', 'C002')
    ]

    order_df = spark.createDataFrame(
        order_data,
        ['orderid', 'itemname', 'priceperunit', 'qty', 'date', 'custid']
    )

    result_df = processor.create_order_summary(customer_df, order_df)

    assert result_df is not None
    assert result_df.count() == 2
    assert 'TotalAmount' in result_df.columns
    assert 'name' in result_df.columns
    assert 'emailid' in result_df.columns
    assert 'region' in result_df.columns

    # Verify TotalAmount calculation
    first_row = result_df.filter(result_df.orderid == 'O001').first()
    assert first_row['TotalAmount'] == 1200.00

    second_row = result_df.filter(result_df.orderid == 'O002').first()
    assert second_row['TotalAmount'] == 51.00  # 25.50 * 2


def test_aggregation_processor_calculates_spend():
    """Test AggregationProcessor calculates customer aggregate spend"""
    from main.job import AggregationProcessor

    logger = Mock()
    processor = AggregationProcessor(logger)

    # Create sample order summary data
    order_summary_data = [
        ('O001', 'Laptop', 1200.00, 1, '2024-01-15', 'C001', 'John Doe', 'john@example.com', 'North', 1200.00),
        ('O002', 'Mouse', 25.50, 2, '2024-01-15', 'C001', 'John Doe', 'john@example.com', 'North', 51.00),
        ('O003', 'Keyboard', 75.00, 1, '2024-01-16', 'C001', 'John Doe', 'john@example.com', 'North', 75.00),
        ('O004', 'Monitor', 300.00, 1, '2024-01-15', 'C002', 'Jane Smith', 'jane@example.com', 'South', 300.00)
    ]

    order_summary_df = spark.createDataFrame(
        order_summary_data,
        ['orderid', 'itemname', 'priceperunit', 'qty', 'date', 'custid', 'name', 'emailid', 'region', 'TotalAmount']
    )

    result_df = processor.calculate_customer_aggregate_spend(order_summary_df)

    assert result_df is not None
    assert result_df.count() == 3  # C001 has 2 dates, C002 has 1 date
    assert 'TotalSpend' in result_df.columns

    # Verify aggregation for C001 on 2024-01-15
    c001_jan15 = result_df.filter(
        (result_df.custid == 'C001') & (result_df.date == '2024-01-15')
    ).first()
    assert c001_jan15['TotalSpend'] == 1251.00  # 1200 + 51

    # Verify aggregation for C001 on 2024-01-16
    c001_jan16 = result_df.filter(
        (result_df.custid == 'C001') & (result_df.date == '2024-01-16')
    ).first()
    assert c001_jan16['TotalSpend'] == 75.00


def test_end_to_end_pipeline():
    """Test end-to-end pipeline execution"""
    from main.job import (
        ConfigManager, S3DataReader, DataQualityTransformer,
        S3DataWriter, OrderSummaryProcessor, AggregationProcessor
    )

    logger = Mock()
    glueContext = Mock()

    # Initialize components
    reader = S3DataReader(spark, logger)
    quality = DataQualityTransformer(logger)
    writer = S3DataWriter(spark, glueContext, logger)
    order_processor = OrderSummaryProcessor(logger)
    agg_processor = AggregationProcessor(logger)

    # Create sample customer data
    customer_data = [
        ('C001', 'John Doe', 'john@example.com', 'North'),
        ('C002', 'Jane Smith', 'jane@example.com', 'South'),
        ('C003', 'Bob Johnson', None, 'East'),  # Will be removed
        ('C001', 'John Doe', 'john@example.com', 'North')  # Duplicate
    ]

    customer_df = spark.createDataFrame(
        customer_data,
        ['CustId', 'Name', 'EmailId', 'Region']
    )

    # Create sample order data
    order_data = [
        ('O001', 'Laptop', 1200.00, 1, '2024-01-15', 'C001'),
        ('O002', 'Mouse', 25.50, 2, '2024-01-16', 'C002'),
        ('O003', 'Keyboard', None, 1, '2024-01-17', 'C001'),  # Will be removed
        ('O001', 'Laptop', 1200.00, 1, '2024-01-15', 'C001')  # Duplicate
    ]

    order_df = spark.createDataFrame(
        order_data,
        ['OrderId', 'ItemName', 'PricePerUnit', 'Qty', 'Date', 'CustId']
    )

    # Mock reader
    with patch.object(reader, '_read_csv_internal', side_effect=[customer_df, order_df]):
        customer_df_read = reader.read_data_safe(
            path='s3://test/customer/',
            expected_schema=['CustId', 'Name', 'EmailId', 'Region'],
            data_type='customer'
        )

        order_df_read = reader.read_data_safe(
            path='s3://test/order/',
            expected_schema=['OrderId', 'ItemName', 'PricePerUnit', 'Qty', 'Date', 'CustId'],
            data_type='order'
        )

    # Clean data
    customer_df_clean = quality.remove_nulls(customer_df_read, 'customer')
    customer_df_final = quality.remove_duplicates(customer_df_clean, 'customer')

    order_df_clean = quality.remove_nulls(order_df_read, 'order')
    order_df_final = quality.remove_duplicates(order_df_clean, 'order')

    # Verify cleaning
    assert customer_df_final.count() == 2  # C001, C002 (C003 removed for null, duplicate removed)
    assert order_df_final.count() == 2  # O001, O002 (O003 removed for null, duplicate removed)

    # Create order summary
    order_summary_df = order_processor.create_order_summary(customer_df_final, order_df_final)
    assert order_summary_df.count() == 2

    # Calculate aggregate spend
    aggregate_spend_df = agg_processor.calculate_customer_aggregate_spend(order_summary_df)
    assert aggregate_spend_df.count() == 2  # One per customer per date

    # Mock writer
    with patch.object(writer, '_write_parquet'):
        writer.write_data_safe(
            df=customer_df_final,
            path='s3://test/customer/',
            table_name='test_customer',
            database_name='test_db',
            data_type='customer'
        )

        writer.write_data_safe(
            df=order_df_final,
            path='s3://test/order/',
            table_name='test_order',
            database_name='test_db',
            data_type='order'
        )

    with patch.object(writer, '_write_hudi'):
        writer.write_hudi_scd2(
            df=order_summary_df,
            path='s3://test/ordersummary/',
            hudi_config={},
            data_type='order_summary'
        )

    with patch.object(writer, '_write_parquet'):
        writer.write_data_safe(
            df=aggregate_spend_df,
            path='s3://test/analytics/',
            table_name='test_analytics',
            database_name='test_db',
            data_type='analytics'
        )


if __name__ == "__main__":
    print("Running tests...")

    test_config_manager_loads_parameters()
    print("✓ test_config_manager_loads_parameters")

    test_s3_data_reader_reads_customer_data()
    print("✓ test_s3_data_reader_reads_customer_data")

    test_s3_data_reader_reads_order_data()
    print("✓ test_s3_data_reader_reads_order_data")

    test_s3_data_reader_validates_schema()
    print("✓ test_s3_data_reader_validates_schema")

    test_data_quality_transformer_removes_nulls()
    print("✓ test_data_quality_transformer_removes_nulls")

    test_data_quality_transformer_removes_duplicates()
    print("✓ test_data_quality_transformer_removes_duplicates")

    test_s3_data_writer_writes_parquet()
    print("✓ test_s3_data_writer_writes_parquet")

    test_s3_data_writer_writes_hudi_scd2()
    print("✓ test_s3_data_writer_writes_hudi_scd2")

    test_order_summary_processor_creates_summary()
    print("✓ test_order_summary_processor_creates_summary")

    test_aggregation_processor_calculates_spend()
    print("✓ test_aggregation_processor_calculates_spend")

    test_end_to_end_pipeline()
    print("✓ test_end_to_end_pipeline")

    print("\nAll tests passed!")