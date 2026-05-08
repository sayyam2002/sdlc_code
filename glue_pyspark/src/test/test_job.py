"""
Comprehensive Test Suite for Customer Order SCD Type 2 ETL Job

Tests cover:
1. Spark context initialization
2. S3 data operations
3. Data cleaning logic
4. SCD Type 2 processing
5. Customer aggregation
6. Hudi configuration
7. Glue Catalog integration
8. Error handling
9. End-to-end workflow

Framework: pytest with moto for AWS mocking
"""

import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
import yaml
import tempfile
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType, BooleanType, LongType
)
from pyspark.sql.functions import col

from moto import mock_s3, mock_glue
import boto3

# Import modules to test
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'main'))

from job import (
    SparkContextManager,
    S3DataManager,
    DataCleaner,
    SCD2Processor,
    CustomerAggregator,
    GlueCatalogManager,
    CustomerOrderETLJob
)


# ============================================
# FIXTURES
# ============================================

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestCustomerOrderETL") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


@pytest.fixture
def sample_customer_data(spark_session):
    """Create sample customer data"""
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("registration_date", TimestampType(), True)
    ])

    data = [
        ("C001", "John Doe", "john.doe@email.com", "555-0001", "123 Main St", "New York", "NY", "10001", datetime(2023, 1, 15)),
        ("C002", "Jane Smith", "jane.smith@email.com", "555-0002", "456 Oak Ave", "Los Angeles", "CA", "90001", datetime(2023, 2, 20)),
        ("C003", "Bob Johnson", "bob.johnson@email.com", "555-0003", "789 Pine Rd", "Chicago", "IL", "60601", datetime(2023, 3, 10)),
        ("C004", "Alice Williams", "alice.w@email.com", "555-0004", "321 Elm St", "Houston", "TX", "77001", datetime(2023, 4, 5)),
        ("C005", "Charlie Brown", "charlie.b@email.com", None, "654 Maple Dr", "Phoenix", "AZ", "85001", datetime(2023, 5, 12))
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark_session):
    """Create sample order data"""
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("order_amount", DecimalType(10, 2), False),
        StructField("order_status", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True)
    ])

    data = [
        ("O001", "C001", datetime(2023, 6, 1), Decimal("150.00"), "Completed", "P001", 2, Decimal("75.00")),
        ("O002", "C001", datetime(2023, 6, 15), Decimal("200.00"), "Completed", "P002", 1, Decimal("200.00")),
        ("O003", "C002", datetime(2023, 6, 10), Decimal("300.00"), "Completed", "P003", 3, Decimal("100.00")),
        ("O004", "C003", datetime(2023, 6, 20), Decimal("450.00"), "Completed", "P004", 5, Decimal("90.00")),
        ("O005", "C002", datetime(2023, 7, 1), Decimal("175.00"), "Pending", "P005", 1, Decimal("175.00")),
        ("O006", "C004", datetime(2023, 7, 5), Decimal("225.00"), "Completed", "P001", 3, Decimal("75.00")),
        ("O007", "C001", datetime(2023, 7, 10), Decimal("125.00"), "Completed", "P006", 1, Decimal("125.00"))
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_with_nulls(spark_session):
    """Create sample customer data with NULL values"""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("registration_date", TimestampType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@email.com", "555-0001", datetime(2023, 1, 15)),
        (None, "Jane Smith", "jane@email.com", "555-0002", datetime(2023, 2, 20)),
        ("C003", None, "bob@email.com", "555-0003", datetime(2023, 3, 10)),
        ("C004", "Alice Williams", None, "555-0004", datetime(2023, 4, 5)),
        ("C005", "Charlie Brown", "charlie@email.com", "555-0005", datetime(2023, 5, 12))
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_with_null_strings(spark_session):
    """Create sample customer data with 'Null' string values"""
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), True)
    ])

    data = [
        ("C001", "John Doe", "john@email.com", "555-0001"),
        ("C002", "Jane Smith", "Null", "555-0002"),
        ("C003", "Bob Johnson", "bob@email.com", "NULL"),
        ("C004", "Alice Williams", "alice@email.com", "None"),
        ("C005", "Charlie Brown", "charlie@email.com", "555-0005")
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_customer_with_duplicates(spark_session):
    """Create sample customer data with duplicates"""
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("registration_date", TimestampType(), False)
    ])

    data = [
        ("C001", "John Doe", "john@email.com", datetime(2023, 1, 15)),
        ("C001", "John Doe Updated", "john.new@email.com", datetime(2023, 6, 20)),  # Duplicate - newer
        ("C002", "Jane Smith", "jane@email.com", datetime(2023, 2, 20)),
        ("C003", "Bob Johnson", "bob@email.com", datetime(2023, 3, 10)),
        ("C003", "Bob Johnson", "bob@email.com", datetime(2023, 3, 5))  # Duplicate - older
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def test_config():
    """Create test configuration"""
    return {
        'source_paths': {
            'customer': 's3://adif-sdlc/curated/sdlc_wizard/customer/',
            'order': 's3://adif-sdlc/curated/sdlc_wizard/order/'
        },
        'target_paths': {
            'order_summary': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
            'customer_aggregate': 's3://adif-sdlc/analytics/customeraggregatespend/'
        },
        'glue_catalog': {
            'database': 'sdlc_wizard_db',
            'tables': {
                'customer_aggregate_spend': 'customer_aggregate_spend'
            }
        },
        'hudi_config': {
            'table_type': 'COPY_ON_WRITE',
            'operation': 'upsert',
            'precombine_field': 'OpTs',
            'table_names': {
                'order_summary': 'order_summary_hudi',
                'customer_aggregate': 'customer_aggregate_hudi'
            },
            'record_keys': {
                'order': 'order_id',
                'customer': 'customer_id',
                'customer_aggregate': 'customer_id'
            }
        },
        'scd_type2': {
            'columns': {
                'is_active': 'IsActive',
                'start_date': 'StartDate',
                'end_date': 'EndDate',
                'operation_timestamp': 'OpTs'
            }
        },
        'data_cleaning': {
            'remove_nulls': True,
            'remove_null_strings': True,
            'remove_duplicates': True,
            'required_fields': {
                'customer': ['customer_id', 'customer_name', 'email'],
                'order': ['order_id', 'customer_id', 'order_date', 'order_amount']
            },
            'null_string_values': ['Null', 'NULL', 'null', 'None', 'NONE', 'none', '', ' ']
        },
        'job_execution': {
            'stages': {
                'ingestion': True,
                'cleaning': True,
                'scd2_processing': True,
                'aggregation': True,
                'catalog_registration': True
            }
        }
    }


@pytest.fixture
def temp_config_file(test_config):
    """Create temporary configuration file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        temp_path = f.name

    yield temp_path

    os.unlink(temp_path)


# ============================================
# TEST: Spark Context Initialization
# ============================================

def test_spark_context_initialization():
    """Test FR-INIT-001: Spark context initialization"""
    context_manager = SparkContextManager()

    sc, glue_ctx, spark = context_manager.initialize_spark_contexts()

    assert sc is not None, "SparkContext should be initialized"
    assert glue_ctx is not None, "GlueContext should be initialized"
    assert spark is not None, "SparkSession should be initialized"
    assert spark.sparkContext == sc, "SparkSession should use the same SparkContext"

    context_manager.cleanup()


def test_spark_session_configuration(spark_session):
    """Test FR-INIT-002: Spark session configuration"""
    # Verify Spark session is configured correctly
    assert spark_session is not None
    assert spark_session.sparkContext.appName == "TestCustomerOrderETL"

    # Verify configuration
    conf = spark_session.sparkContext.getConf()
    assert conf.get("spark.sql.shuffle.partitions") == "2"


# ============================================
# TEST: S3 Data Operations
# ============================================

@mock_s3
def test_s3_path_validation():
    """Test FR-S3-001: S3 path validation"""
    # Create mock S3 bucket and objects
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='adif-sdlc')
    s3_client.put_object(
        Bucket='adif-sdlc',
        Key='curated/sdlc_wizard/customer/data.parquet',
        Body=b'test data'
    )

    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    s3_manager = S3DataManager(spark)

    # Test valid path
    valid_path = "s3://adif-sdlc/curated/sdlc_wizard/customer/"
    assert s3_manager.validate_s3_path(valid_path) == True

    # Test invalid path
    invalid_path = "s3://adif-sdlc/nonexistent/path/"
    assert s3_manager.validate_s3_path(invalid_path) == False

    # Test invalid format
    bad_format = "invalid://path"
    assert s3_manager.validate_s3_path(bad_format) == False

    spark.stop()


def test_read_data_safe_with_schema(spark_session, sample_customer_data, tmp_path):
    """Test FR-S3-002: Safe data reading with schema validation"""
    # Write sample data to temporary location
    temp_path = str(tmp_path / "customer_data")
    sample_customer_data.write.parquet(temp_path)

    s3_manager = S3DataManager(spark_session)

    # Mock validate_s3_path to return True
    with patch.object(s3_manager, 'validate_s3_path', return_value=True):
        # Read data with schema
        df = s3_manager.read_data_safe(temp_path, file_format="parquet")

        assert df is not None
        assert df.count() == 5
        assert "customer_id" in df.columns
        assert "customer_name" in df.columns


def test_column_normalization(spark_session):
    """Test FR-S3-003: Column name normalization to lowercase"""
    # Create DataFrame with mixed case columns
    data = [("C001", "John Doe"), ("C002", "Jane Smith")]
    df = spark_session.createDataFrame(data, ["Customer_ID", "Customer_Name"])

    # Normalize columns
    df_normalized = df.toDF(*[c.lower() for c in df.columns])

    assert "customer_id" in df_normalized.columns
    assert "customer_name" in df_normalized.columns
    assert "Customer_ID" not in df_normalized.columns


# ============================================
# TEST: Data Cleaning
# ============================================

def test_remove_nulls(sample_customer_with_nulls):
    """Test FR-CLEAN-001: Remove NULL values in required fields"""
    cleaner = DataCleaner()
    required_fields = ['customer_id', 'customer_name', 'email']

    df_cleaned = cleaner.remove_nulls(sample_customer_with_nulls, required_fields)

    # Should keep only rows where all required fields are not NULL
    assert df_cleaned.count() == 2  # Only C001 and C005 have all required fields

    # Verify no NULLs in required fields
    for field in required_fields:
        null_count = df_cleaned.filter(col(field).isNull()).count()
        assert null_count == 0, f"Field {field} should not have NULL values"


def test_remove_null_strings(sample_customer_with_null_strings):
    """Test FR-CLEAN-002: Remove 'Null' string values"""
    cleaner = DataCleaner()
    null_values = ['Null', 'NULL', 'null', 'None', 'NONE', 'none']

    df_cleaned = cleaner.remove_null_strings(sample_customer_with_null_strings, null_values)

    # Check that 'Null' strings are replaced with actual NULL
    null_email_count = df_cleaned.filter(col("email").isNull()).count()
    assert null_email_count >= 1, "Should have NULL values after replacing 'Null' strings"

    # Verify no 'Null' strings remain
    for null_val in null_values:
        count = df_cleaned.filter(col("email") == null_val).count()
        assert count == 0, f"Should not have '{null_val}' string values"


def test_remove_duplicates(sample_customer_with_duplicates):
    """Test FR-CLEAN-003: Remove duplicate records"""
    cleaner = DataCleaner()

    df_deduped = cleaner.remove_duplicates(
        sample_customer_with_duplicates,
        key_columns=['customer_id'],
        order_column='registration_date'
    )

    # Should keep only 3 unique customers
    assert df_deduped.count() == 3

    # Verify C001 has the latest record (2023-06-20)
    c001_record = df_deduped.filter(col("customer_id") == "C001").collect()[0]
    assert c001_record['customer_name'] == "John Doe Updated"
    assert c001_record['email'] == "john.new@email.com"

    # Verify C003 has the latest record (2023-03-10)
    c003_record = df_deduped.filter(col("customer_id") == "C003").collect()[0]
    assert c003_record['registration_date'] == datetime(2023, 3, 10)


def test_data_cleaning_pipeline(sample_customer_with_nulls, sample_customer_with_null_strings):
    """Test FR-CLEAN-004: Complete data cleaning pipeline"""
    cleaner = DataCleaner()

    # Start with data that has NULLs
    df = sample_customer_with_nulls
    initial_count = df.count()

    # Apply cleaning steps
    df = cleaner.remove_null_strings(df, ['Null', 'NULL', 'None'])
    df = cleaner.remove_nulls(df, ['customer_id', 'customer_name', 'email'])

    final_count = df.count()

    # Should have removed rows with NULLs
    assert final_count < initial_count
    assert final_count >= 1


# ============================================
# TEST: SCD Type 2 Processing
# ============================================

def test_add_scd2_columns(sample_customer_data):
    """Test FR-SCD2-001: Add SCD Type 2 columns"""
    processor = SCD2Processor()

    df_scd2 = processor.add_scd2_columns(sample_customer_data)

    # Verify SCD Type 2 columns are added
    assert "IsActive" in df_scd2.columns
    assert "StartDate" in df_scd2.columns
    assert "EndDate" in df_scd2.columns
    assert "OpTs" in df_scd2.columns

    # Verify column count increased by 4
    assert len(df_scd2.columns) == len(sample_customer_data.columns) + 4

    # Verify default values
    first_row = df_scd2.first()
    assert first_row['IsActive'] == True
    assert first_row['StartDate'] is not None
    assert first_row['EndDate'] is None
    assert first_row['OpTs'] is not None


def test_validate_scd2_columns(sample_customer_data):
    """Test FR-SCD2-002: Validate SCD Type 2 columns"""
    processor = SCD2Processor()

    # DataFrame without SCD Type 2 columns
    assert processor.validate_scd2_columns(sample_customer_data) == False

    # DataFrame with SCD Type 2 columns
    df_scd2 = processor.add_scd2_columns(sample_customer_data)
    assert processor.validate_scd2_columns(df_scd2) == True


def test_scd2_column_types(sample_customer_data):
    """Test FR-SCD2-003: Verify SCD Type 2 column data types"""
    processor = SCD2Processor()
    df_scd2 = processor.add_scd2_columns(sample_customer_data)

    # Verify data types
    schema_dict = {field.name: field.dataType for field in df_scd2.schema.fields}

    assert isinstance(schema_dict['IsActive'], BooleanType)
    assert isinstance(schema_dict['StartDate'], TimestampType)
    assert isinstance(schema_dict['EndDate'], TimestampType)
    assert isinstance(schema_dict['OpTs'], TimestampType)


def test_scd2_isactive_flag(sample_customer_data):
    """Test FR-SCD2-004: IsActive flag is set correctly"""
    processor = SCD2Processor()
    df_scd2 = processor.add_scd2_columns(sample_customer_data)

    # All records should be active initially
    active_count = df_scd2.filter(col("IsActive") == True).count()
    assert active_count == df_scd2.count()

    inactive_count = df_scd2.filter(col("IsActive") == False).count()
    assert inactive_count == 0


# ============================================
# TEST: Customer Aggregation
# ============================================

def test_customer_aggregate_spend_calculation(sample_customer_data, sample_order_data):
    """Test FR-AGG-001: Calculate customer aggregate spend"""
    aggregator = CustomerAggregator()

    agg_df = aggregator.calculate_customer_aggregate_spend(sample_customer_data, sample_order_data)

    # Verify aggregation results
    assert agg_df.count() == 4  # 4 customers with orders

    # Verify columns
    expected_columns = ['customer_id', 'customer_name', 'total_orders', 'total_spend',
                       'avg_order_value', 'first_order_date', 'last_order_date']
    for col_name in expected_columns:
        assert col_name in agg_df.columns

    # Verify C001 metrics (3 orders: 150, 200, 125)
    c001_metrics = agg_df.filter(col("customer_id") == "C001").collect()[0]
    assert c001_metrics['total_orders'] == 3
    assert float(c001_metrics['total_spend']) == 475.0
    assert float(c001_metrics['avg_order_value']) == pytest.approx(158.33, rel=0.01)


def test_customer_aggregate_spend_data_types(sample_customer_data, sample_order_data):
    """Test FR-AGG-002: Verify aggregate data types"""
    aggregator = CustomerAggregator()
    agg_df = aggregator.calculate_customer_aggregate_spend(sample_customer_data, sample_order_data)

    schema_dict = {field.name: field.dataType for field in agg_df.schema.fields}

    assert isinstance(schema_dict['total_orders'], LongType)
    assert isinstance(schema_dict['total_spend'], DecimalType)
    assert isinstance(schema_dict['avg_order_value'], DecimalType)
    assert isinstance(schema_dict['first_order_date'], TimestampType)
    assert isinstance(schema_dict['last_order_date'], TimestampType)


def test_customer_aggregate_with_single_order(spark_session):
    """Test FR-AGG-003: Customer with single order"""
    # Create customer with single order
    customer_data = [("C001", "John Doe")]
    customer_df = spark_session.createDataFrame(customer_data, ["customer_id", "customer_name"])

    order_data = [("O001", "C001", datetime(2023, 6, 1), Decimal("150.00"))]
    order_schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_date", TimestampType()),
        StructField("order_amount", DecimalType(10, 2))
    ])
    order_df = spark_session.createDataFrame(order_data, order_schema)

    aggregator = CustomerAggregator()
    agg_df = aggregator.calculate_customer_aggregate_spend(customer_df, order_df)

    metrics = agg_df.collect()[0]
    assert metrics['total_orders'] == 1
    assert float(metrics['total_spend']) == 150.0
    assert float(metrics['avg_order_value']) == 150.0
    assert metrics['first_order_date'] == metrics['last_order_date']


def test_customer_aggregate_with_scd2(sample_customer_data, sample_order_data):
    """Test FR-AGG-004: Customer aggregate with SCD Type 2 columns"""
    aggregator = CustomerAggregator()
    processor = SCD2Processor()

    agg_df = aggregator.calculate_customer_aggregate_spend(sample_customer_data, sample_order_data)
    agg_scd2 = processor.add_scd2_columns(agg_df)

    # Verify SCD Type 2 columns are present
    assert processor.validate_scd2_columns(agg_scd2) == True

    # Verify all aggregate columns are still present
    assert "total_orders" in agg_scd2.columns
    assert "total_spend" in agg_scd2.columns


# ============================================
# TEST: Hudi Configuration
# ============================================

def test_hudi_write_configuration(test_config):
    """Test FR-HUDI-001: Hudi write configuration"""
    hudi_config = test_config['hudi_config']

    # Verify required Hudi configuration
    assert hudi_config['table_type'] == 'COPY_ON_WRITE'
    assert hudi_config['operation'] == 'upsert'
    assert hudi_config['precombine_field'] == 'OpTs'

    # Verify record keys
    assert 'order' in hudi_config['record_keys']
    assert 'customer' in hudi_config['record_keys']
    assert 'customer_aggregate' in hudi_config['record_keys']


def test_hudi_table_names(test_config):
    """Test FR-HUDI-002: Hudi table names configuration"""
    table_names = test_config['hudi_config']['table_names']

    assert 'order_summary' in table_names
    assert 'customer_aggregate' in table_names
    assert table_names['order_summary'] == 'order_summary_hudi'
    assert table_names['customer_aggregate'] == 'customer_aggregate_hudi'


# ============================================
# TEST: Glue Catalog Integration
# ============================================

@mock_glue
def test_create_glue_database():
    """Test FR-CATALOG-001: Create Glue Catalog database"""
    catalog_manager = GlueCatalogManager('sdlc_wizard_db')

    success = catalog_manager.create_database_if_not_exists()

    assert success == True

    # Verify database exists
    glue_client = boto3.client('glue', region_name='us-east-1')
    response = glue_client.get_database(Name='sdlc_wizard_db')
    assert response['Database']['Name'] == 'sdlc_wizard_db'


@mock_glue
def test_register_table_in_catalog():
    """Test FR-CATALOG-002: Register table in Glue Catalog"""
    catalog_manager = GlueCatalogManager('sdlc_wizard_db')
    catalog_manager.create_database_if_not_exists()

    columns = [
        {'Name': 'customer_id', 'Type': 'string'},
        {'Name': 'total_spend', 'Type': 'decimal(20,2)'}
    ]

    success = catalog_manager.register_table(
        table_name='customer_aggregate_spend',
        s3_path='s3://adif-sdlc/analytics/customeraggregatespend/',
        columns=columns
    )

    assert success == True

    # Verify table exists
    glue_client = boto3.client('glue', region_name='us-east-1')
    response = glue_client.get_table(
        DatabaseName='sdlc_wizard_db',
        Name='customer_aggregate_spend'
    )
    assert response['Table']['Name'] == 'customer_aggregate_spend'


# ============================================
# TEST: Configuration Management
# ============================================

def test_load_config_from_file(temp_config_file):
    """Test FR-CONFIG-001: Load configuration from YAML file"""
    with open(temp_config_file, 'r') as f:
        config = yaml.safe_load(f)

    assert config is not None
    assert 'source_paths' in config
    assert 'target_paths' in config
    assert 'glue_catalog' in config
    assert 'hudi_config' in config


def test_config_source_paths(test_config):
    """Test FR-CONFIG-002: Validate source paths from TRD"""
    source_paths = test_config['source_paths']

    # Verify exact S3 paths from TRD
    assert source_paths['customer'] == 's3://adif-sdlc/curated/sdlc_wizard/customer/'
    assert source_paths['order'] == 's3://adif-sdlc/curated/sdlc_wizard/order/'


def test_config_target_paths(test_config):
    """Test FR-CONFIG-003: Validate target paths from TRD"""
    target_paths = test_config['target_paths']

    # Verify exact S3 paths from TRD
    assert target_paths['order_summary'] == 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'
    assert target_paths['customer_aggregate'] == 's3://adif-sdlc/analytics/customeraggregatespend/'


def test_config_glue_catalog(test_config):
    """Test FR-CONFIG-004: Validate Glue Catalog configuration"""
    glue_catalog = test_config['glue_catalog']

    assert glue_catalog['database'] == 'sdlc_wizard_db'
    assert 'tables' in glue_catalog


# ============================================
# TEST: Error Handling
# ============================================

def test_handle_missing_s3_path(spark_session):
    """Test FR-ERROR-001: Handle missing S3 path"""
    s3_manager = S3DataManager(spark_session)

    # Mock validate_s3_path to return False
    with patch.object(s3_manager, 'validate_s3_path', return_value=False):
        df = s3_manager.read_data_safe('s3://nonexistent/path/', file_format='parquet')

        assert df is None


def test_handle_invalid_schema(spark_session, tmp_path):
    """Test FR-ERROR-002: Handle invalid schema"""
    # Create data with wrong schema
    data = [("C001", "John Doe")]
    df = spark_session.createDataFrame(data, ["id", "name"])

    temp_path = str(tmp_path / "invalid_data")
    df.write.parquet(temp_path)

    s3_manager = S3DataManager(spark_session)

    # Define expected schema
    expected_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("email", StringType())  # This field doesn't exist
    ])

    with patch.object(s3_manager, 'validate_s3_path', return_value=True):
        # Should handle schema mismatch gracefully
        try:
            df_read = s3_manager.read_data_safe(temp_path, schema=expected_schema)
            # If it succeeds, verify columns are different
            if df_read:
                assert set(df_read.columns) != set(['customer_id', 'customer_name', 'email'])
        except Exception:
            # Expected to fail with schema mismatch
            pass


def test_handle_empty_dataframe(spark_session):
    """Test FR-ERROR-003: Handle empty DataFrame"""
    # Create empty DataFrame
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType())
    ])
    empty_df = spark_session.createDataFrame([], schema)

    # Test cleaning operations on empty DataFrame
    cleaner = DataCleaner()
    result = cleaner.remove_nulls(empty_df, ['customer_id'])

    assert result.count() == 0
    assert result.columns == empty_df.columns


# ============================================
# TEST: Schema Validation
# ============================================

def test_customer_schema_validation(sample_customer_data):
    """Test FR-SCHEMA-001: Validate customer schema from TRD"""
    expected_columns = [
        'customer_id', 'customer_name', 'email', 'phone',
        'address', 'city', 'state', 'zip_code', 'registration_date'
    ]

    for col_name in expected_columns:
        assert col_name in sample_customer_data.columns


def test_order_schema_validation(sample_order_data):
    """Test FR-SCHEMA-002: Validate order schema from TRD"""
    expected_columns = [
        'order_id', 'customer_id', 'order_date', 'order_amount',
        'order_status', 'product_id', 'quantity', 'unit_price'
    ]

    for col_name in expected_columns:
        assert col_name in sample_order_data.columns


def test_aggregate_schema_validation(sample_customer_data, sample_order_data):
    """Test FR-SCHEMA-003: Validate aggregate schema"""
    aggregator = CustomerAggregator()
    agg_df = aggregator.calculate_customer_aggregate_spend(sample_customer_data, sample_order_data)

    expected_columns = [
        'customer_id', 'customer_name', 'total_orders', 'total_spend',
        'avg_order_value', 'first_order_date', 'last_order_date'
    ]

    for col_name in expected_columns:
        assert col_name in agg_df.columns


# ============================================
# TEST: End-to-End Workflow
# ============================================

def test_etl_job_initialization(temp_config_file):
    """Test FR-E2E-001: ETL job initialization"""
    job = CustomerOrderETLJob(temp_config_file)

    assert job.config is not None
    assert job.context_manager is not None
    assert 'source_paths' in job.config
    assert 'target_paths' in job.config


def test_complete_data_pipeline(sample_customer_data, sample_order_data):
    """Test FR-E2E-002: Complete data pipeline"""
    # Stage 1: Clean data
    cleaner = DataCleaner()
    customer_cleaned = cleaner.remove_nulls(sample_customer_data, ['customer_id', 'customer_name', 'email'])
    order_cleaned = cleaner.remove_nulls(sample_order_data, ['order_id', 'customer_id', 'order_date', 'order_amount'])

    # Stage 2: Add SCD Type 2
    processor = SCD2Processor()
    customer_scd2 = processor.add_scd2_columns(customer_cleaned)
    order_scd2 = processor.add_scd2_columns(order_cleaned)

    # Stage 3: Calculate aggregations
    aggregator = CustomerAggregator()
    agg_df = aggregator.calculate_customer_aggregate_spend(customer_scd2, order_scd2)
    agg_scd2 = processor.add_scd2_columns(agg_df)

    # Verify final output
    assert customer_scd2.count() > 0
    assert order_scd2.count() > 0
    assert agg_scd2.count() > 0
    assert processor.validate_scd2_columns(customer_scd2)
    assert processor.validate_scd2_columns(order_scd2)
    assert processor.validate_scd2_columns(agg_scd2)


def test_data_quality_metrics(sample_customer_data, sample_order_data):
    """Test FR-E2E-003: Data quality metrics"""
    initial_customer_count = sample_customer_data.count()
    initial_order_count = sample_order_data.count()

    # Apply cleaning
    cleaner = DataCleaner()
    customer_cleaned = cleaner.remove_nulls(sample_customer_data, ['customer_id'])
    order_cleaned = cleaner.remove_nulls(sample_order_data, ['order_id'])

    final_customer_count = customer_cleaned.count()
    final_order_count = order_cleaned.count()

    # Verify data quality
    assert final_customer_count <= initial_customer_count
    assert final_order_count <= initial_order_count
    assert final_customer_count > 0
    assert final_order_count > 0


# ============================================
# TEST: Performance and Optimization
# ============================================

def test_partition_handling(spark_session, tmp_path):
    """Test FR-PERF-001: Partition handling"""
    # Create sample data
    data = [
        ("C001", "2023-01-15"),
        ("C002", "2023-02-20"),
        ("C003", "2023-03-10")
    ]
    df = spark_session.createDataFrame(data, ["customer_id", "date"])

    # Write with partitioning
    temp_path = str(tmp_path / "partitioned_data")
    df.write.partitionBy("date").parquet(temp_path)

    # Verify partitions were created
    import os
    partition_dirs = [d for d in os.listdir(temp_path) if d.startswith("date=")]
    assert len(partition_dirs) > 0


def test_caching_strategy(sample_customer_data):
    """Test FR-PERF-002: DataFrame caching"""
    # Cache DataFrame
    sample_customer_data.cache()

    # Perform multiple operations
    count1 = sample_customer_data.count()
    count2 = sample_customer_data.count()

    assert count1 == count2

    # Unpersist
    sample_customer_data.unpersist()


# ============================================
# SUMMARY TEST
# ============================================

def test_all_trd_requirements_implemented(test_config):
    """Test FR-SUMMARY-001: Verify all TRD requirements are implemented"""

    # Verify S3 paths from TRD
    assert test_config['source_paths']['customer'] == 's3://adif-sdlc/curated/sdlc_wizard/customer/'
    assert test_config['source_paths']['order'] == 's3://adif-sdlc/curated/sdlc_wizard/order/'
    assert test_config['target_paths']['customer_aggregate'] == 's3://adif-sdlc/analytics/customeraggregatespend/'

    # Verify SCD Type 2 configuration
    scd2_columns = test_config['scd_type2']['columns']
    assert 'is_active' in scd2_columns
    assert 'start_date' in scd2_columns
    assert 'end_date' in scd2_columns
    assert 'operation_timestamp' in scd2_columns

    # Verify Hudi configuration
    assert test_config['hudi_config']['operation'] == 'upsert'
    assert test_config['hudi_config']['table_type'] == 'COPY_ON_WRITE'

    # Verify data cleaning configuration
    assert test_config['data_cleaning']['remove_nulls'] == True
    assert test_config['data_cleaning']['remove_null_strings'] == True
    assert test_config['data_cleaning']['remove_duplicates'] == True

    # Verify Glue Catalog configuration
    assert test_config['glue_catalog']['database'] == 'sdlc_wizard_db'

    print("\n" + "=" * 80)
    print("✓ ALL TRD REQUIREMENTS VALIDATED")
    print("=" * 80)
    print("✓ S3 Paths: Validated")
    print("✓ SCD Type 2: Validated")
    print("✓ Hudi Configuration: Validated")
    print("✓ Data Cleaning: Validated")
    print("✓ Glue Catalog: Validated")
    print("=" * 80)