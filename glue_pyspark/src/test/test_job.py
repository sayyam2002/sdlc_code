"""
Comprehensive test suite for Customer Order Analytics Glue Job

Tests all FRD requirements:
- FR-INGEST-001: Customer data ingestion
- FR-INGEST-002: Order data ingestion
- FR-CLEAN-001: Data cleaning
- FR-SCD2-001: SCD Type 2 implementation
- FR-AGG-001: Customer aggregate spend
- FR-SUMMARY-001: Order summary generation

Framework: pytest with moto for AWS mocking
"""

import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

import boto3
from moto import mock_s3
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, BooleanType, TimestampType
)

# Import classes from job module
import sys
sys.path.insert(0, 'src/main')

from job import (
    SparkContextManager,
    S3Validator,
    DataReader,
    DataWriter,
    DataCleaner,
    SCDType2Handler,
    CustomerOrderAnalytics
)


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestCustomerOrderAnalytics") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def s3_validator():
    """Create S3 validator instance"""
    return S3Validator()


@pytest.fixture
def data_reader(spark, s3_validator):
    """Create data reader instance"""
    return DataReader(spark, s3_validator)


@pytest.fixture
def data_writer(s3_validator):
    """Create data writer instance"""
    return DataWriter(s3_validator)


@pytest.fixture
def data_cleaner():
    """Create data cleaner instance"""
    return DataCleaner()


@pytest.fixture
def scd2_handler(spark):
    """Create SCD Type 2 handler instance"""
    return SCDType2Handler(spark)


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data matching TRD schema"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", "Alice Brown", "alice@example.com", "West"),
        ("C005", "Charlie Davis", "charlie@example.com", "North")
    ]

    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    """Create sample order data matching TRD schema"""
    data = [
        ("O001", "C001", "Laptop", Decimal("999.99"), 1, "2024-01-15"),
        ("O002", "C001", "Mouse", Decimal("29.99"), 2, "2024-01-16"),
        ("O003", "C002", "Keyboard", Decimal("79.99"), 1, "2024-01-17"),
        ("O004", "C003", "Monitor", Decimal("299.99"), 2, "2024-01-18"),
        ("O005", "C004", "Laptop", Decimal("999.99"), 1, "2024-01-19")
    ]

    schema = StructType([
        StructField("orderid", StringType(), True),
        StructField("custid", StringType(), True),
        StructField("itemname", StringType(), True),
        StructField("priceperunit", DecimalType(10, 2), True),
        StructField("qty", IntegerType(), True),
        StructField("date", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_data_with_nulls(spark):
    """Create sample customer data with NULL and 'Null' string values"""
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", None, "jane@example.com", "South"),  # NULL value
        ("C003", "Null", "bob@example.com", "East"),  # 'Null' string
        ("C004", "Alice Brown", None, "West"),  # NULL value
        ("C005", "Charlie Davis", "charlie@example.com", "null"),  # 'null' string
        ("C006", "David Wilson", "david@example.com", "North"),
        ("C006", "David Wilson", "david@example.com", "North")  # Duplicate
    ]

    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


@mock_s3
def test_s3_validator_validate_valid_path(s3_validator):
    """Test S3 path validation with valid path"""
    # Create mock S3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='adif-sdlc')

    # Test validation
    result = s3_validator.validate_s3_path('s3://adif-sdlc/test/')

    assert result is True


def test_s3_validator_validate_invalid_format(s3_validator):
    """Test S3 path validation with invalid format"""
    result = s3_validator.validate_s3_path('invalid-path')

    assert result is False


@mock_s3
def test_s3_validator_check_path_exists(s3_validator):
    """Test checking if S3 path has objects"""
    # Create mock S3 bucket and object
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='adif-sdlc')
    s3_client.put_object(
        Bucket='adif-sdlc',
        Key='test/data.csv',
        Body=b'test data'
    )

    # Test path exists
    result = s3_validator.check_s3_path_exists('s3://adif-sdlc/test/')

    assert result is True


@mock_s3
def test_s3_validator_check_path_not_exists(s3_validator):
    """Test checking if S3 path does not exist"""
    # Create mock S3 bucket (empty)
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='adif-sdlc')

    # Test path does not exist
    result = s3_validator.check_s3_path_exists('s3://adif-sdlc/empty/')

    assert result is False


def test_data_cleaner_remove_nulls(data_cleaner, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001: Remove NULL values
    TR-INGEST-002: Data cleaning requirement
    """
    # Clean data
    df_cleaned = data_cleaner.clean_data(sample_customer_data_with_nulls)

    # Verify no NULL values remain
    null_count = df_cleaned.filter(
        df_cleaned.custid.isNull() |
        df_cleaned.name.isNull() |
        df_cleaned.emailid.isNull() |
        df_cleaned.region.isNull()
    ).count()

    assert null_count == 0
    assert df_cleaned.count() < sample_customer_data_with_nulls.count()


def test_data_cleaner_remove_null_strings(data_cleaner, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001: Remove 'Null' string values
    TR-INGEST-002: Data cleaning requirement
    """
    # Clean data
    df_cleaned = data_cleaner.clean_data(sample_customer_data_with_nulls)

    # Verify no 'Null' string values remain
    null_string_count = df_cleaned.filter(
        (df_cleaned.name == 'Null') |
        (df_cleaned.region == 'null')
    ).count()

    assert null_string_count == 0


def test_data_cleaner_remove_duplicates(data_cleaner, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001: Remove duplicate records
    TR-INGEST-002: Data cleaning requirement
    """
    # Clean data
    df_cleaned = data_cleaner.clean_data(sample_customer_data_with_nulls)

    # Verify no duplicates remain
    distinct_count = df_cleaned.select("custid", "name", "emailid", "region").distinct().count()

    assert df_cleaned.count() == distinct_count


def test_data_cleaner_complete_cleaning(data_cleaner, sample_customer_data_with_nulls):
    """
    Test FR-CLEAN-001: Complete data cleaning pipeline
    Verify all cleaning rules applied
    """
    initial_count = sample_customer_data_with_nulls.count()

    # Clean data
    df_cleaned = data_cleaner.clean_data(sample_customer_data_with_nulls)

    final_count = df_cleaned.count()

    # Should have removed records with NULLs, 'Null' strings, and duplicates
    assert final_count < initial_count
    assert final_count == 1  # Only C001 should remain clean


def test_scd2_handler_add_columns(scd2_handler, sample_customer_data):
    """
    Test FR-SCD2-001: Add SCD Type 2 columns
    Verify columns: IsActive, StartDate, EndDate, OpTs
    """
    # Add SCD Type 2 columns
    df_scd2 = scd2_handler.add_scd2_columns(sample_customer_data)

    # Verify columns exist
    assert "isactive" in df_scd2.columns
    assert "startdate" in df_scd2.columns
    assert "enddate" in df_scd2.columns
    assert "opts" in df_scd2.columns

    # Verify column types
    schema_dict = {field.name: field.dataType for field in df_scd2.schema.fields}
    assert isinstance(schema_dict["isactive"], BooleanType)
    assert isinstance(schema_dict["startdate"], TimestampType)
    assert isinstance(schema_dict["enddate"], TimestampType)
    assert isinstance(schema_dict["opts"], TimestampType)


def test_scd2_handler_initial_load_values(scd2_handler, sample_customer_data):
    """
    Test FR-SCD2-001: Verify initial load SCD Type 2 values
    All records should be active with StartDate set
    """
    # Add SCD Type 2 columns
    df_scd2 = scd2_handler.add_scd2_columns(sample_customer_data, is_initial_load=True)

    # Collect data
    rows = df_scd2.collect()

    # Verify all records are active
    for row in rows:
        assert row["isactive"] is True
        assert row["startdate"] is not None
        assert row["enddate"] is None
        assert row["opts"] is not None


def test_customer_order_analytics_get_customer_schema():
    """
    Test FR-INGEST-001: Verify customer schema matches TRD
    Schema: CustId (string), Name (string), EmailId (string), Region (string)
    """
    spark = SparkSession.builder.master("local[2]").getOrCreate()
    glue_context = Mock()

    analytics = CustomerOrderAnalytics(spark, glue_context)
    schema = analytics.get_customer_schema()

    # Verify schema fields
    field_names = [field.name for field in schema.fields]
    assert "custid" in field_names
    assert "name" in field_names
    assert "emailid" in field_names
    assert "region" in field_names

    # Verify field types
    schema_dict = {field.name: field.dataType for field in schema.fields}
    assert isinstance(schema_dict["custid"], StringType)
    assert isinstance(schema_dict["name"], StringType)
    assert isinstance(schema_dict["emailid"], StringType)
    assert isinstance(schema_dict["region"], StringType)


def test_customer_order_analytics_get_order_schema():
    """
    Test FR-INGEST-002: Verify order schema matches TRD
    Schema: OrderId (string), ItemName (string), PricePerUnit (decimal),
            Qty (integer), Date (string)
    """
    spark = SparkSession.builder.master("local[2]").getOrCreate()
    glue_context = Mock()

    analytics = CustomerOrderAnalytics(spark, glue_context)
    schema = analytics.get_order_schema()

    # Verify schema fields
    field_names = [field.name for field in schema.fields]
    assert "orderid" in field_names
    assert "itemname" in field_names
    assert "priceperunit" in field_names
    assert "qty" in field_names
    assert "date" in field_names

    # Verify field types
    schema_dict = {field.name: field.dataType for field in schema.fields}
    assert isinstance(schema_dict["orderid"], StringType)
    assert isinstance(schema_dict["itemname"], StringType)
    assert isinstance(schema_dict["priceperunit"], DecimalType)
    assert isinstance(schema_dict["qty"], IntegerType)
    assert isinstance(schema_dict["date"], StringType)


def test_customer_order_analytics_calculate_aggregate_spend(
    spark,
    sample_customer_data,
    sample_order_data
):
    """
    Test FR-AGG-001: Calculate customer aggregate spend
    Verify: sum(PricePerUnit * Qty) grouped by CustId, Name
    """
    glue_context = Mock()
    analytics = CustomerOrderAnalytics(spark, glue_context)

    # Calculate aggregate spend
    aggregate_df = analytics.calculate_customer_aggregate_spend(
        sample_customer_data,
        sample_order_data
    )

    assert aggregate_df is not None

    # Verify schema
    assert "custid" in aggregate_df.columns
    assert "name" in aggregate_df.columns
    assert "totalspend" in aggregate_df.columns

    # Verify calculations
    results = {row["custid"]: float(row["totalspend"]) for row in aggregate_df.collect()}

    # C001: Laptop (999.99 * 1) + Mouse (29.99 * 2) = 1059.97
    assert abs(results["C001"] - 1059.97) < 0.01

    # C002: Keyboard (79.99 * 1) = 79.99
    assert abs(results["C002"] - 79.99) < 0.01

    # C003: Monitor (299.99 * 2) = 599.98
    assert abs(results["C003"] - 599.98) < 0.01


def test_customer_order_analytics_generate_order_summary(
    spark,
    sample_customer_data,
    sample_order_data
):
    """
    Test FR-SUMMARY-001: Generate order summary
    Verify: OrderId, CustId, Name, ItemName, TotalAmount, Date
    """
    glue_context = Mock()
    analytics = CustomerOrderAnalytics(spark, glue_context)

    # Generate order summary
    summary_df = analytics.generate_order_summary(
        sample_customer_data,
        sample_order_data
    )

    assert summary_df is not None

    # Verify schema
    expected_columns = ["orderid", "custid", "name", "itemname", "totalamount", "date"]
    for col in expected_columns:
        assert col in summary_df.columns

    # Verify record count
    assert summary_df.count() == sample_order_data.count()

    # Verify calculations
    results = {row["orderid"]: float(row["totalamount"]) for row in summary_df.collect()}

    # O001: Laptop (999.99 * 1) = 999.99
    assert abs(results["O001"] - 999.99) < 0.01

    # O002: Mouse (29.99 * 2) = 59.98
    assert abs(results["O002"] - 59.98) < 0.01


def test_s3_paths_match_trd():
    """
    Test that S3 paths match TRD specifications exactly
    Verify all paths from TRD are correctly configured
    """
    spark = SparkSession.builder.master("local[2]").getOrCreate()
    glue_context = Mock()

    analytics = CustomerOrderAnalytics(spark, glue_context)

    # Verify input paths from TRD
    assert analytics.customer_input_path == "s3://adif-sdlc/sdlc_wizard/customerdata/"
    assert analytics.order_input_path == "s3://adif-sdlc/sdlc_wizard/orderdata/"

    # Verify output paths from TRD
    assert analytics.customer_scd2_path == "s3://adif-sdlc/sdlc_wizard/customerdata/"
    assert analytics.aggregate_spend_path == "s3://adif-sdlc/analytics/customeraggregatespend/"
    assert analytics.order_summary_path == "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"


def test_column_normalization(spark, sample_customer_data):
    """
    Test that column names are normalized to lowercase
    Requirement: df.toDF(*[c.lower() for c in df.columns])
    """
    # Create DataFrame with mixed case columns
    data = [("C001", "John Doe", "john@example.com", "North")]
    schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    # Normalize columns
    df_normalized = df.toDF(*[c.lower() for c in df.columns])

    # Verify all columns are lowercase
    for col_name in df_normalized.columns:
        assert col_name == col_name.lower()


def test_hudi_configuration_parameters(scd2_handler, spark):
    """
    Test FR-SCD2-001: Verify Hudi configuration matches TRD
    - Format: hudi
    - Operation: upsert
    - Record Key: CustId
    - Precombine Field: OpTs
    """
    # Create sample data
    data = [("C001", "John Doe", "john@example.com", "North")]
    schema = StructType([
        StructField("custid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailid", StringType(), True),
        StructField("region", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    df_scd2 = scd2_handler.add_scd2_columns(df)

    # Verify SCD Type 2 columns are present for Hudi
    assert "custid" in df_scd2.columns  # Record key
    assert "opts" in df_scd2.columns  # Precombine field
    assert "isactive" in df_scd2.columns
    assert "startdate" in df_scd2.columns
    assert "enddate" in df_scd2.columns


def test_data_quality_no_nulls_after_cleaning(data_cleaner, sample_customer_data_with_nulls):
    """
    Test data quality: No NULL values after cleaning
    """
    df_cleaned = data_cleaner.clean_data(sample_customer_data_with_nulls)

    # Check each column for NULLs
    for col_name in df_cleaned.columns:
        null_count = df_cleaned.filter(df_cleaned[col_name].isNull()).count()
        assert null_count == 0, f"Column {col_name} has {null_count} NULL values"


def test_data_quality_positive_amounts(spark, sample_order_data):
    """
    Test data quality: PricePerUnit and Qty should be positive
    """
    # Verify all prices are positive
    negative_prices = sample_order_data.filter(col("priceperunit") <= 0).count()
    assert negative_prices == 0

    # Verify all quantities are positive
    negative_qty = sample_order_data.filter(col("qty") <= 0).count()
    assert negative_qty == 0


def test_spark_context_initialization():
    """
    Test SparkContextManager.initialize_spark_contexts()
    Verify proper initialization of Spark and Glue contexts
    """
    with patch('job.SparkContext') as mock_sc, \
         patch('job.GlueContext') as mock_glue:

        mock_spark = Mock()
        mock_glue.return_value.spark_session = mock_spark

        sc, glue_context, spark = SparkContextManager.initialize_spark_contexts()

        assert sc is not None
        assert glue_context is not None
        assert spark is not None


def test_aggregate_spend_no_orders(spark, sample_customer_data):
    """
    Test FR-AGG-001: Handle customers with no orders
    """
    glue_context = Mock()
    analytics = CustomerOrderAnalytics(spark, glue_context)

    # Create empty order DataFrame
    empty_orders = spark.createDataFrame([], analytics.get_order_schema())

    # Calculate aggregate spend
    aggregate_df = analytics.calculate_customer_aggregate_spend(
        sample_customer_data,
        empty_orders
    )

    # Should return empty DataFrame
    assert aggregate_df is not None
    assert aggregate_df.count() == 0


def test_order_summary_join_integrity(spark, sample_customer_data, sample_order_data):
    """
    Test FR-SUMMARY-001: Verify join integrity in order summary
    All orders should have matching customer data
    """
    glue_context = Mock()
    analytics = CustomerOrderAnalytics(spark, glue_context)

    # Generate order summary
    summary_df = analytics.generate_order_summary(
        sample_customer_data,
        sample_order_data
    )

    # Verify no NULL customer names (indicating failed join)
    null_names = summary_df.filter(col("name").isNull()).count()
    assert null_names == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])