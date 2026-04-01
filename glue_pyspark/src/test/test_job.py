import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../main')))

from job import (
    get_customer_schema,
    get_order_schema,
    clean_data,
    join_customer_order,
    detect_changed_customers
)


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("TestGlueJob") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    schema = get_customer_schema()
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East")
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_data(spark):
    schema = get_order_schema()
    data = [
        ("O001", "Laptop", 1200.0, 1, "2024-01-15", "C001"),
        ("O002", "Mouse", 25.0, 2, "2024-01-16", "C001"),
        ("O003", "Keyboard", 75.0, 1, "2024-01-17", "C002")
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_customer_data(spark):
    schema = get_customer_schema()
    data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "Null", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C003", "Bob Johnson", "bob@example.com", "East"),
        ("C004", None, "test@example.com", "West")
    ]
    return spark.createDataFrame(data, schema)


def test_get_customer_schema():
    schema = get_customer_schema()
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4
    assert schema.fields[0].name == "CustId"
    assert schema.fields[1].name == "Name"
    assert schema.fields[2].name == "EmailId"
    assert schema.fields[3].name == "Region"


def test_get_order_schema():
    schema = get_order_schema()
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 6
    assert schema.fields[0].name == "OrderId"
    assert schema.fields[1].name == "ItemName"
    assert schema.fields[2].name == "PricePerUnit"
    assert schema.fields[3].name == "Qty"
    assert schema.fields[4].name == "Date"
    assert schema.fields[5].name == "CustId"


def test_clean_data_removes_nulls(spark, dirty_customer_data):
    cleaned_df = clean_data(dirty_customer_data, "customer")
    assert cleaned_df.count() == 2

    null_count = cleaned_df.filter(
        (cleaned_df.Name.isNull()) |
        (cleaned_df.Name == "Null") |
        (cleaned_df.EmailId.isNull()) |
        (cleaned_df.EmailId == "Null")
    ).count()
    assert null_count == 0


def test_clean_data_removes_duplicates(spark, dirty_customer_data):
    cleaned_df = clean_data(dirty_customer_data, "customer")

    c003_count = cleaned_df.filter(cleaned_df.CustId == "C003").count()
    assert c003_count == 1


def test_join_customer_order(spark, sample_customer_data, sample_order_data):
    joined_df = join_customer_order(sample_customer_data, sample_order_data)

    assert joined_df.count() == 3

    assert "TotalAmount" in joined_df.columns
    assert "Name" in joined_df.columns
    assert "EmailId" in joined_df.columns
    assert "Region" in joined_df.columns

    total_amounts = joined_df.select("TotalAmount").collect()
    expected_amounts = [1200.0, 50.0, 75.0]
    actual_amounts = sorted([row.TotalAmount for row in total_amounts])
    assert actual_amounts == sorted(expected_amounts)


def test_detect_changed_customers_first_run(spark, sample_customer_data):
    changed = detect_changed_customers(sample_customer_data, None)

    assert changed.count() == sample_customer_data.count()


def test_detect_changed_customers_no_changes(spark, sample_customer_data):
    changed = detect_changed_customers(sample_customer_data, sample_customer_data)

    assert changed.count() == 0


def test_detect_changed_customers_with_changes(spark):
    schema = get_customer_schema()

    previous_data = [
        ("C001", "John Doe", "john@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South")
    ]
    previous_df = spark.createDataFrame(previous_data, schema)

    current_data = [
        ("C001", "John Doe", "john.new@example.com", "North"),
        ("C002", "Jane Smith", "jane@example.com", "South"),
        ("C003", "Bob Johnson", "bob@example.com", "East")
    ]
    current_df = spark.createDataFrame(current_data, schema)

    changed = detect_changed_customers(current_df, previous_df)

    assert changed.count() == 2

    changed_ids = [row.CustId for row in changed.select("CustId").collect()]
    assert "C001" in changed_ids
    assert "C003" in changed_ids


def test_join_preserves_all_columns(spark, sample_customer_data, sample_order_data):
    joined_df = join_customer_order(sample_customer_data, sample_order_data)

    expected_columns = [
        "OrderId", "ItemName", "PricePerUnit", "Qty", "Date", "CustId",
        "Name", "EmailId", "Region", "TotalAmount"
    ]

    for col in expected_columns:
        assert col in joined_df.columns


def test_clean_data_preserves_valid_records(spark, sample_customer_data):
    cleaned_df = clean_data(sample_customer_data, "customer")

    assert cleaned_df.count() == sample_customer_data.count()


def test_total_amount_calculation(spark, sample_customer_data, sample_order_data):
    joined_df = join_customer_order(sample_customer_data, sample_order_data)

    laptop_order = joined_df.filter(joined_df.OrderId == "O001").collect()[0]
    assert laptop_order.TotalAmount == 1200.0

    mouse_order = joined_df.filter(joined_df.OrderId == "O002").collect()[0]
    assert mouse_order.TotalAmount == 50.0

    keyboard_order = joined_df.filter(joined_df.OrderId == "O003").collect()[0]
    assert keyboard_order.TotalAmount == 75.0