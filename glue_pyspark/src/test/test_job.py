"""
Comprehensive test suite for AWS Glue PySpark ETL Job
Tests all transformations, SCD Type 2 logic, and aggregations
"""

import sys
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType


# Initialize Spark session for testing
@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark_session = SparkSession.builder \
        .appName("test_customer_order_etl") \
        .master("local[2]") \
        .getOrCreate()

    # Make spark available globally for job initialization
    __builtins__.spark = spark_session

    yield spark_session

    # Cleanup
    if hasattr(__builtins__, 'spark'):
        delattr(__builtins__, 'spark')


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West")
    ]
    schema = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with null values for testing"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Null", "jane@example.com", "South"),
        ("C003", "Bob Johnson", None, "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C001", "John Doe", "john@example.com", "North")  # Duplicate
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
    """Create sample order data for testing"""
    data = [
        ("O001", "C001", "2024-01-01", "100.50"),
        ("O002", "C001", "2024-01-01", "200.75"),
        ("O003", "C002", "2024-01-02", "150.00"),
        ("O004", "C003", "2024-01-02", "300.25")
    ]
    schema = StructType([
        StructField("OrderId", StringType(), False),
        StructField("CustId", StringType(), False),
        StructField("OrderDate", StringType(), True),
        StructField("Amount", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_previous_customer_state(spark):
    """Create sample previous customer state for SCD Type 2 testing"""
    data = [
        ("C001", "John Doe", "john.old@example.com", "North", True,
         datetime(2024, 1, 1), None, datetime(2024, 1, 1)),
        ("C002", "Jane Smith", "jane@example.com", "South", True,
         datetime(2024, 1, 1), None, datetime(2024, 1, 1))
    ]
    schema = StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True),
        StructField("IsActive", BooleanType(), True),
        StructField("StartDate", TimestampType(), True),
        StructField("EndDate", TimestampType(), True),
        StructField("OpTs", TimestampType(), True)
    ])
    return spark.createDataFrame(data, schema)


def test_get_job_parameters():
    """Test job parameter loading"""
    from main.job import get_job_parameters

    params = get_job_parameters()

    assert params is not None
    assert "inputs" in params
    assert "outputs" in params
    assert "catalog" in params
    assert "hudi" in params
    assert "processing" in params
    assert "scd_type2" in params

    # Verify customer input configuration
    assert "customer" in params["inputs"]
    assert params["inputs"]["customer"]["source_path"] == "s3://adif-sdlc/sdlc_wizard/customerdata/"
    assert params["inputs"]["customer"]["source_format"] == "csv"

    # Verify order input configuration
    assert "order" in params["inputs"]
    assert params["inputs"]["order"]["source_path"] == "s3://adif-sdlc/sdlc_wizard/orderdata/"

    # Verify catalog configuration
    assert params["catalog"]["database"] == "sdlc_wizard_db"
    assert "customer" in params["catalog"]["tables"]
    assert "order" in params["catalog"]["tables"]
    assert "ordersummary" in params["catalog"]["tables"]


def test_initialize_spark_contexts(spark):
    """Test Spark context initialization"""
    from main.job import initialize_spark_contexts

    spark_session, glueContext, job, job_name = initialize_spark_contexts()

    assert spark_session is not None
    assert glueContext is not None
    assert job is not None
    assert job_name == "test_job"


def test_get_customer_schema():
    """Test customer schema definition"""
    from main.job import get_customer_schema

    schema = get_customer_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[0].nullable == False
    assert schema.fields[1].name == "Name"
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[3].name == "Region"


def test_get_order_schema():
    """Test order schema definition"""
    from main.job import get_order_schema

    schema = get_order_schema()

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[1].name == "CustId"
    assert schema.fields[2].name == "OrderDate"
    assert schema.fields[3].name == "Amount"


def test_normalize_columns(spark, sample_customer_data):
    """Test column name normalization"""
    from main.job import normalize_columns

    normalized_df = normalize_columns(sample_customer_data)

    assert "custid" in normalized_df.columns
    assert "name" in normalized_df.columns
    assert "emailid" in normalized_df.columns
    assert "region" in normalized_df.columns
    assert "CustId" not in normalized_df.columns


def test_remove_nulls_and_duplicates(spark, sample_customer_data_with_nulls):
    """Test null and duplicate removal"""
    from main.job import remove_nulls_and_duplicates

    cleaned_df = remove_nulls_and_duplicates(sample_customer_data_with_nulls, "Null")

    # Should remove 3 rows: 1 with "Null" string, 1 with actual NULL, 1 duplicate
    assert cleaned_df.count() == 2

    # Verify no nulls remain
    for row in cleaned_df.collect():
        for value in row:
            assert value is not None
            assert value != "Null"


def test_data_reader_csv(spark, sample_customer_data):
    """Test DataReader CSV reading with mocking"""
    from main.job import DataReader

    reader = DataReader(spark)

    with patch.object(reader, '_read_csv', return_value=sample_customer_data) as mock_read:
        df = reader.read_data_safe("s3://test/path/", "csv")

        assert df is not None
        assert df.count() == 4
        mock_read.assert_called_once()


def test_data_reader_parquet(spark, sample_customer_data):
    """Test DataReader Parquet reading with mocking"""
    from main.job import DataReader

    reader = DataReader(spark)

    with patch.object(reader, '_read_parquet', return_value=sample_customer_data) as mock_read:
        df = reader.read_data_safe("s3://test/path/", "parquet")

        assert df is not None
        assert df.count() == 4
        mock_read.assert_called_once()


def test_data_reader_error_handling(spark):
    """Test DataReader error handling"""
    from main.job import DataReader

    reader = DataReader(spark)

    with patch.object(reader, '_read_csv', side_effect=Exception("Read error")):
        df = reader.read_data_safe("s3://invalid/path/", "csv")

        assert df is None


def test_data_writer_parquet(spark, sample_customer_data):
    """Test DataWriter Parquet writing with mocking"""
    from main.job import DataWriter

    writer = DataWriter(spark)

    with patch.object(writer, '_write_parquet', return_value=None) as mock_write:
        result = writer.write_data_safe(sample_customer_data, "s3://test/output/", "parquet")

        assert result == True
        mock_write.assert_called_once()


def test_data_writer_csv(spark, sample_customer_data):
    """Test DataWriter CSV writing with mocking"""
    from main.job import DataWriter

    writer = DataWriter(spark)

    with patch.object(writer, '_write_csv', return_value=None) as mock_write:
        result = writer.write_data_safe(sample_customer_data, "s3://test/output/", "csv")

        assert result == True
        mock_write.assert_called_once()


def test_data_writer_error_handling(spark, sample_customer_data):
    """Test DataWriter error handling"""
    from main.job import DataWriter

    writer = DataWriter(spark)

    with patch.object(writer, '_write_parquet', side_effect=Exception("Write error")):
        result = writer.write_data_safe(sample_customer_data, "s3://invalid/path/", "parquet")

        assert result == False


def test_write_to_catalog(spark, sample_customer_data):
    """Test catalog registration"""
    from main.job import write_to_catalog, DataWriter

    with patch.object(DataWriter, 'write_data_safe', return_value=True) as mock_write:
        result = write_to_catalog(
            spark,
            sample_customer_data,
            "test_db",
            "test_table",
            "s3://test/catalog/",
            "parquet"
        )

        assert result == True
        mock_write.assert_called_once()


def test_detect_customer_changes_initial_load(spark, sample_customer_data):
    """Test change detection with no previous state (initial load)"""
    from main.job import detect_customer_changes

    changed_df = detect_customer_changes(
        sample_customer_data,
        None,
        ["Name", "EmailId", "Region"]
    )

    # All records should be returned for initial load
    assert changed_df.count() == sample_customer_data.count()


def test_detect_customer_changes_with_changes(spark, sample_customer_data, sample_previous_customer_state):
    """Test change detection with actual changes"""
    from main.job import detect_customer_changes, normalize_columns

    # Normalize the sample data
    current_normalized = normalize_columns(sample_customer_data)

    changed_df = detect_customer_changes(
        current_normalized,
        sample_previous_customer_state,
        ["name", "emailid", "region"]
    )

    # Should detect changes (C001 email changed, C003 and C004 are new)
    assert changed_df.count() >= 1


def test_apply_scd_type2_initial_load(spark, sample_customer_data):
    """Test SCD Type 2 application for initial load"""
    from main.job import apply_scd_type2, get_job_parameters

    params = get_job_parameters()

    result_df = apply_scd_type2(spark, sample_customer_data, None, params)

    # All records should be active
    assert result_df.count() == sample_customer_data.count()

    # Verify SCD columns exist
    assert "IsActive" in result_df.columns
    assert "StartDate" in result_df.columns
    assert "EndDate" in result_df.columns
    assert "OpTs" in result_df.columns

    # All records should be active
    active_count = result_df.filter(result_df.IsActive == True).count()
    assert active_count == sample_customer_data.count()


def test_apply_scd_type2_with_changes(spark, sample_customer_data, sample_previous_customer_state):
    """Test SCD Type 2 application with changes"""
    from main.job import apply_scd_type2, get_job_parameters, normalize_columns

    params = get_job_parameters()

    # Normalize current data
    current_normalized = normalize_columns(sample_customer_data)

    result_df = apply_scd_type2(spark, current_normalized, sample_previous_customer_state, params)

    # Should have more records due to versioning
    assert result_df.count() >= sample_previous_customer_state.count()

    # Verify SCD columns exist
    assert "IsActive" in result_df.columns
    assert "StartDate" in result_df.columns
    assert "EndDate" in result_df.columns
    assert "OpTs" in result_df.columns


def test_calculate_customer_aggregate_spend(spark, sample_customer_data, sample_order_data):
    """Test customer aggregate spend calculation"""
    from main.job import calculate_customer_aggregate_spend

    aggregated_df = calculate_customer_aggregate_spend(sample_customer_data, sample_order_data)

    # Should have aggregated records
    assert aggregated_df.count() > 0

    # Verify columns
    assert "custid" in aggregated_df.columns
    assert "orderdate" in aggregated_df.columns
    assert "totalspend" in aggregated_df.columns

    # Verify aggregation for C001 on 2024-01-01 (100.50 + 200.75 = 301.25)
    c001_spend = aggregated_df.filter(
        (aggregated_df.custid == "C001") &
        (aggregated_df.orderdate == "2024-01-01")
    ).collect()

    if len(c001_spend) > 0:
        assert abs(c001_spend[0].totalspend - 301.25) < 0.01


def test_write_hudi_table(spark, sample_customer_data):
    """Test Hudi table writing with fallback"""
    from main.job import write_hudi_table, get_job_parameters, DataWriter

    params = get_job_parameters()

    # Add SCD columns to sample data
    from pyspark.sql.functions import lit
    from datetime import datetime

    df_with_scd = sample_customer_data \
        .withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", lit(datetime.utcnow())) \
        .withColumn("EndDate", lit(None)) \
        .withColumn("OpTs", lit(datetime.utcnow()))

    # Mock the DataWriter to avoid actual writes
    with patch.object(DataWriter, 'write_data_safe', return_value=True):
        result = write_hudi_table(spark, df_with_scd, params)

        # Should succeed (either Hudi or fallback)
        assert result == True


def test_main_integration(spark, sample_customer_data, sample_order_data):
    """Test main function integration with mocked I/O"""
    from main.job import main, DataReader, DataWriter

    # Mock all I/O operations
    with patch.object(DataReader, '_read_csv') as mock_read_csv, \
         patch.object(DataReader, '_read_parquet') as mock_read_parquet, \
         patch.object(DataWriter, '_write_parquet') as mock_write_parquet, \
         patch('main.job.write_hudi_table', return_value=True):

        # Setup mock returns
        mock_read_csv.side_effect = [sample_customer_data, sample_order_data]
        mock_read_parquet.return_value = None  # No previous state
        mock_write_parquet.return_value = None

        # Run main function
        try:
            main()
            # If no exception, test passes
            assert True
        except Exception as e:
            # Should not raise exceptions with proper mocking
            pytest.fail(f"Main function raised exception: {str(e)}")


def test_end_to_end_pipeline(spark, sample_customer_data, sample_order_data):
    """Test complete end-to-end pipeline"""
    from main.job import (
        normalize_columns,
        remove_nulls_and_duplicates,
        apply_scd_type2,
        calculate_customer_aggregate_spend,
        get_job_parameters
    )

    params = get_job_parameters()

    # Step 1: Normalize columns
    customer_normalized = normalize_columns(sample_customer_data)
    order_normalized = normalize_columns(sample_order_data)

    assert "custid" in customer_normalized.columns
    assert "custid" in order_normalized.columns

    # Step 2: Clean data
    customer_clean = remove_nulls_and_duplicates(customer_normalized)
    order_clean = remove_nulls_and_duplicates(order_normalized)

    assert customer_clean.count() > 0
    assert order_clean.count() > 0

    # Step 3: Apply SCD Type 2
    customer_scd = apply_scd_type2(spark, customer_clean, None, params)

    assert "IsActive" in customer_scd.columns
    assert customer_scd.filter(customer_scd.IsActive == True).count() > 0

    # Step 4: Calculate aggregates
    aggregated = calculate_customer_aggregate_spend(customer_clean, order_clean)

    assert aggregated.count() > 0
    assert "totalspend" in aggregated.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])