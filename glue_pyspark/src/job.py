"""
AWS Glue PySpark Job - SDLC Wizard Data Processing
Implements data ingestion, cleaning, and SCD Type 2 using Apache Hudi
"""

import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, sum as _sum,
    count, avg, max as _max, min as _min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, BooleanType, TimestampType
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def get_customer_schema():
    """
    Define customer data schema based on TRD specifications
    """
    return StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])


def get_order_schema():
    """
    Define order data schema based on TRD specifications
    """
    return StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DecimalType(10, 2), True),
        StructField("Qty", IntegerType(), True),
        StructField("Date", StringType(), True)
    ])


def read_customer_data(spark, input_path):
    """
    Read customer data from S3

    Args:
        spark: SparkSession
        input_path: S3 path to customer data

    Returns:
        DataFrame: Customer data
    """
    customer_schema = get_customer_schema()

    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .schema(customer_schema) \
        .csv(input_path)

    return df


def read_order_data(spark, input_path):
    """
    Read order data from S3

    Args:
        spark: SparkSession
        input_path: S3 path to order data

    Returns:
        DataFrame: Order data
    """
    order_schema = get_order_schema()

    df = spark.read \
        .option("header", "true") \
        .schema(order_schema) \
        .csv(input_path)

    return df


def clean_dataframe(df):
    """
    Clean DataFrame by removing NULL values, 'Null' strings, and duplicates

    Args:
        df: Input DataFrame

    Returns:
        DataFrame: Cleaned DataFrame
    """
    # Remove rows with NULL values in any column
    df_cleaned = df.dropna()

    # Remove rows with 'Null' string values in any column
    for column in df.columns:
        df_cleaned = df_cleaned.filter(
            (col(column) != "Null") &
            (col(column) != "null") &
            (col(column) != "NULL")
        )

    # Remove duplicate records
    df_cleaned = df_cleaned.dropDuplicates()

    return df_cleaned


def add_scd2_columns(df, record_key):
    """
    Add SCD Type 2 columns to DataFrame

    Args:
        df: Input DataFrame
        record_key: Primary key column name

    Returns:
        DataFrame: DataFrame with SCD Type 2 columns
    """
    current_ts = current_timestamp()

    df_scd2 = df.withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", current_ts) \
        .withColumn("EndDate", lit(None).cast(TimestampType())) \
        .withColumn("OpTs", current_ts)

    return df_scd2


def write_hudi_table(df, output_path, table_name, record_key, precombine_field):
    """
    Write DataFrame to S3 using Hudi format with SCD Type 2 support

    Args:
        df: Input DataFrame
        output_path: S3 output path
        table_name: Hudi table name
        record_key: Primary key field
        precombine_field: Field for record versioning
    """
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.precombine.field': precombine_field,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(output_path)


def calculate_customer_aggregate_spend(customer_df, order_df):
    """
    Calculate customer aggregate spend metrics

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame

    Returns:
        DataFrame: Customer aggregate spend metrics
    """
    # Calculate total spend per order
    order_with_total = order_df.withColumn(
        "TotalSpend",
        col("PricePerUnit") * col("Qty")
    )

    # Aggregate by customer (assuming CustId exists in order data or needs join)
    # Since TRD doesn't specify CustId in order schema, we aggregate at order level
    aggregate_df = order_with_total.groupBy("OrderId").agg(
        _sum("TotalSpend").alias("TotalOrderSpend"),
        count("ItemName").alias("ItemCount"),
        avg("PricePerUnit").alias("AvgPricePerUnit"),
        _max("Qty").alias("MaxQty"),
        _min("Qty").alias("MinQty")
    )

    return aggregate_df


def main():
    """
    Main execution function for AWS Glue job
    """
    # Get job parameters
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME',
            'customer_input_path',
            'order_input_path',
            'customer_output_path',
            'order_output_path',
            'analytics_output_path'
        ]
    )

    # Initialize Spark and Glue contexts
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    try:
        # FR-INGEST-001: Read customer data
        print(f"Reading customer data from {args['customer_input_path']}")
        customer_df = read_customer_data(spark, args['customer_input_path'])
        print(f"Customer records read: {customer_df.count()}")

        # FR-INGEST-001: Read order data
        print(f"Reading order data from {args['order_input_path']}")
        order_df = read_order_data(spark, args['order_input_path'])
        print(f"Order records read: {order_df.count()}")

        # FR-CLEAN-001: Clean customer data
        print("Cleaning customer data...")
        customer_cleaned = clean_dataframe(customer_df)
        print(f"Customer records after cleaning: {customer_cleaned.count()}")

        # FR-CLEAN-001: Clean order data
        print("Cleaning order data...")
        order_cleaned = clean_dataframe(order_df)
        print(f"Order records after cleaning: {order_cleaned.count()}")

        # FR-SCD2-001: Add SCD Type 2 columns to customer data
        print("Adding SCD Type 2 columns to customer data...")
        customer_scd2 = add_scd2_columns(customer_cleaned, "CustId")

        # FR-SCD2-001: Add SCD Type 2 columns to order data
        print("Adding SCD Type 2 columns to order data...")
        order_scd2 = add_scd2_columns(order_cleaned, "OrderId")

        # FR-SCD2-001: Write customer data to Hudi
        print(f"Writing customer data to {args['customer_output_path']}")
        write_hudi_table(
            customer_scd2,
            args['customer_output_path'],
            'sdlc_wizard_customer',
            'CustId',
            'OpTs'
        )

        # FR-SCD2-001: Write order data to Hudi
        print(f"Writing order data to {args['order_output_path']}")
        write_hudi_table(
            order_scd2,
            args['order_output_path'],
            'sdlc_wizard_ordersummary',
            'OrderId',
            'OpTs'
        )

        # FR-AGGREGATE-001: Calculate customer aggregate spend
        print("Calculating customer aggregate spend...")
        aggregate_spend = calculate_customer_aggregate_spend(
            customer_cleaned,
            order_cleaned
        )
        print(f"Aggregate records calculated: {aggregate_spend.count()}")

        # Write aggregate data
        print(f"Writing aggregate data to {args['analytics_output_path']}")
        aggregate_spend.write \
            .mode("overwrite") \
            .parquet(args['analytics_output_path'])

        print("Job completed successfully")

    except Exception as e:
        print(f"Job failed with error: {str(e)}")
        raise

    finally:
        # Commit job
        job.commit()


if __name__ == "__main__":
    main()