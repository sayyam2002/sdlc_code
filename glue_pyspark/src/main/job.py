"""
AWS Glue PySpark Job - Customer Order Data Processing with SCD Type 2

This job processes customer and order data from S3, applies data cleaning,
performs aggregations, and implements SCD Type 2 using Apache Hudi format.

Data Sources:
- Customer Data: s3://adif-sdlc/sdlc_wizard/customerdata/
- Order Data: s3://adif-sdlc/sdlc_wizard/orderdata/

Outputs:
- Curated Customer Data (SCD2): s3://adif-sdlc/curated/sdlc_wizard/
- Order Summary: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
- Customer Aggregate Spend: s3://adif-sdlc/analytics/customeraggregatespend/
"""

import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, count, lit, current_timestamp,
    to_timestamp, when, max as _max, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType
)


def get_customer_schema():
    """
    Define customer data schema based on TRD requirements.

    Returns:
        StructType: Customer schema with CustId, Name, EmailId, Region
    """
    return StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])


def get_order_schema():
    """
    Define order data schema based on TRD requirements.

    Returns:
        StructType: Order schema with OrderId, CustId, OrderDate, Amount
    """
    return StructType([
        StructField("OrderId", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("OrderDate", StringType(), True),
        StructField("Amount", DoubleType(), True)
    ])


def read_csv_data(spark, path, schema):
    """
    Read CSV data from S3 with specified schema.

    Args:
        spark: SparkSession
        path: S3 path to CSV files
        schema: StructType schema definition

    Returns:
        DataFrame: Raw data from CSV
    """
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(path)


def clean_data(df):
    """
    Clean data by removing NULL values, 'Null' strings, and duplicates.

    Business Rules (from TRD):
    - Remove rows with NULL values in any column
    - Remove rows with 'Null' string values
    - Remove duplicate records

    Args:
        df: Input DataFrame

    Returns:
        DataFrame: Cleaned data
    """
    cleaned_df = df.dropna()

    for column in df.columns:
        cleaned_df = cleaned_df.filter(
            (col(column) != "Null") &
            (col(column) != "null") &
            (col(column) != "NULL")
        )

    cleaned_df = cleaned_df.dropDuplicates()

    return cleaned_df


def add_scd2_columns(df, record_key):
    """
    Add SCD Type 2 columns to DataFrame.

    SCD Type 2 Columns (from TRD):
    - IsActive: boolean (True for current records)
    - StartDate: timestamp (record effective start date)
    - EndDate: timestamp (record effective end date, NULL for active)
    - OpTs: timestamp (operation timestamp)

    Args:
        df: Input DataFrame
        record_key: Primary key column name

    Returns:
        DataFrame: DataFrame with SCD2 columns
    """
    current_ts = current_timestamp()

    df_with_scd2 = df \
        .withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", current_ts) \
        .withColumn("EndDate", lit(None).cast(TimestampType())) \
        .withColumn("OpTs", current_ts)

    return df_with_scd2


def apply_scd2_logic(new_df, existing_df, record_key):
    """
    Apply SCD Type 2 upsert logic.

    Logic:
    1. Identify changed records by comparing new vs existing
    2. Mark existing records as inactive (IsActive=False, set EndDate)
    3. Insert new records as active (IsActive=True)
    4. Keep unchanged records as-is

    Args:
        new_df: New incoming data with SCD2 columns
        existing_df: Existing data from previous runs (None if first run)
        record_key: Primary key column name

    Returns:
        DataFrame: Merged DataFrame with SCD2 logic applied
    """
    if existing_df is None or existing_df.count() == 0:
        return new_df

    current_ts = current_timestamp()

    business_columns = [c for c in new_df.columns
                       if c not in ['IsActive', 'StartDate', 'EndDate', 'OpTs']]

    existing_active = existing_df.filter(col("IsActive") == True)

    changed_keys = new_df.alias("new").join(
        existing_active.alias("old"),
        col(f"new.{record_key}") == col(f"old.{record_key}"),
        "inner"
    )

    for bc in business_columns:
        if bc != record_key:
            changed_keys = changed_keys.filter(
                col(f"new.{bc}") != col(f"old.{bc}")
            )

    changed_key_list = changed_keys.select(f"new.{record_key}").distinct()

    expired_records = existing_active.join(
        changed_key_list,
        record_key,
        "inner"
    ).withColumn("IsActive", lit(False)) \
     .withColumn("EndDate", current_ts) \
     .withColumn("OpTs", current_ts)

    unchanged_records = existing_active.join(
        changed_key_list,
        record_key,
        "left_anti"
    )

    new_records = new_df.join(
        existing_active.select(record_key),
        record_key,
        "left_anti"
    )

    changed_records = new_df.join(
        changed_key_list,
        record_key,
        "inner"
    )

    historical_inactive = existing_df.filter(col("IsActive") == False)

    result_df = expired_records \
        .union(unchanged_records) \
        .union(new_records) \
        .union(changed_records) \
        .union(historical_inactive)

    return result_df


def write_hudi_data(df, path, table_name, record_key, precombine_field):
    """
    Write DataFrame to S3 in Hudi format with SCD Type 2 configuration.

    Hudi Configuration (from TRD):
    - Format: hudi
    - Operation: upsert
    - Record key: Primary key field
    - Precombine field: OpTs (timestamp for conflict resolution)

    Args:
        df: DataFrame to write
        path: S3 output path
        table_name: Hudi table name
        record_key: Primary key column
        precombine_field: Field for conflict resolution (OpTs)
    """
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.precombine.field': precombine_field,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.hive_style_partitioning': 'false',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(path)


def generate_order_summary(spark, customer_df, order_df):
    """
    Generate order summary by joining customer and order data.

    Business Logic:
    - Join orders with customer details
    - Include: OrderId, CustId, Name, EmailId, Region, OrderDate, Amount

    Args:
        spark: SparkSession
        customer_df: Cleaned customer DataFrame
        order_df: Cleaned order DataFrame

    Returns:
        DataFrame: Order summary with customer details
    """
    active_customers = customer_df.filter(col("IsActive") == True)

    order_summary = order_df.alias("o").join(
        active_customers.alias("c"),
        col("o.CustId") == col("c.CustId"),
        "inner"
    ).select(
        col("o.OrderId"),
        col("o.CustId"),
        col("c.Name"),
        col("c.EmailId"),
        col("c.Region"),
        col("o.OrderDate"),
        col("o.Amount")
    )

    return order_summary


def calculate_customer_aggregate_spend(order_df):
    """
    Calculate customer aggregate spend metrics.

    Aggregations (from TRD):
    - Total spend per customer (sum of Amount)
    - Order count per customer
    - Group by CustId

    Args:
        order_df: Cleaned order DataFrame

    Returns:
        DataFrame: Customer aggregate spend with TotalSpend and OrderCount
    """
    aggregate_spend = order_df.groupBy("CustId").agg(
        _sum("Amount").alias("TotalSpend"),
        count("OrderId").alias("OrderCount")
    )

    return aggregate_spend


def write_parquet_data(df, path, mode="overwrite"):
    """
    Write DataFrame to S3 in Parquet format.

    Args:
        df: DataFrame to write
        path: S3 output path
        mode: Write mode (overwrite/append)
    """
    df.write \
        .mode(mode) \
        .parquet(path)


def main():
    """
    Main execution function for AWS Glue job.

    Process Flow:
    1. Initialize Glue context and job
    2. Read customer and order data from S3
    3. Clean data (remove NULLs, 'Null' strings, duplicates)
    4. Apply SCD Type 2 logic to customer data
    5. Write customer data to Hudi format
    6. Generate order summary
    7. Calculate customer aggregate spend
    8. Write outputs to S3
    9. Commit job
    """
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'customer_source_path',
        'order_source_path',
        'curated_customer_path',
        'order_summary_path',
        'aggregate_spend_path'
    ])

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    customer_source_path = args.get('customer_source_path',
                                    's3://adif-sdlc/sdlc_wizard/customerdata/')
    order_source_path = args.get('order_source_path',
                                 's3://adif-sdlc/sdlc_wizard/orderdata/')
    curated_customer_path = args.get('curated_customer_path',
                                     's3://adif-sdlc/curated/sdlc_wizard/')
    order_summary_path = args.get('order_summary_path',
                                  's3://adif-sdlc/curated/sdlc_wizard/ordersummary/')
    aggregate_spend_path = args.get('aggregate_spend_path',
                                    's3://adif-sdlc/analytics/customeraggregatespend/')

    print(f"Reading customer data from: {customer_source_path}")
    customer_schema = get_customer_schema()
    raw_customer_df = read_csv_data(spark, customer_source_path, customer_schema)

    print(f"Reading order data from: {order_source_path}")
    order_schema = get_order_schema()
    raw_order_df = read_csv_data(spark, order_source_path, order_schema)

    print("Cleaning customer data...")
    cleaned_customer_df = clean_data(raw_customer_df)

    print("Cleaning order data...")
    cleaned_order_df = clean_data(raw_order_df)

    print("Adding SCD Type 2 columns to customer data...")
    customer_with_scd2 = add_scd2_columns(cleaned_customer_df, "CustId")

    print("Applying SCD Type 2 logic...")
    try:
        existing_customer_df = spark.read.format("hudi").load(curated_customer_path)
    except Exception as e:
        print(f"No existing customer data found (first run): {e}")
        existing_customer_df = None

    final_customer_df = apply_scd2_logic(
        customer_with_scd2,
        existing_customer_df,
        "CustId"
    )

    print(f"Writing customer data to Hudi format: {curated_customer_path}")
    write_hudi_data(
        final_customer_df,
        curated_customer_path,
        "customer_data",
        "CustId",
        "OpTs"
    )

    print("Generating order summary...")
    order_summary_df = generate_order_summary(
        spark,
        final_customer_df,
        cleaned_order_df
    )

    print(f"Writing order summary: {order_summary_path}")
    write_parquet_data(order_summary_df, order_summary_path)

    print("Calculating customer aggregate spend...")
    aggregate_spend_df = calculate_customer_aggregate_spend(cleaned_order_df)

    print(f"Writing customer aggregate spend: {aggregate_spend_path}")
    write_parquet_data(aggregate_spend_df, aggregate_spend_path)

    print("Job completed successfully")
    job.commit()


if __name__ == "__main__":
    main()