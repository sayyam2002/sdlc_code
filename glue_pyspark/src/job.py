import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, lit, current_timestamp,
    when, to_timestamp, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# Define schemas based on TRD
CUSTOMER_SCHEMA = StructType([
    StructField("CustId", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("EmailId", StringType(), True),
    StructField("Region", StringType(), True)
])

ORDER_SCHEMA = StructType([
    StructField("OrderId", StringType(), True),
    StructField("ItemName", StringType(), True),
    StructField("PricePerUnit", DoubleType(), True),
    StructField("Qty", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("CustId", StringType(), True)
])


def read_customer_data(spark, customer_source_path):
    """
    Read customer data from S3 with CSV and Parquet support.

    Args:
        spark: SparkSession
        customer_source_path: S3 path to customer data

    Returns:
        DataFrame with customer data
    """
    # Try reading as CSV first (with comma delimiter as per TRD)
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("delimiter", ",") \
            .schema(CUSTOMER_SCHEMA) \
            .csv(customer_source_path)
        return df
    except:
        # Fallback to Parquet format as per TRD
        df = spark.read \
            .schema(CUSTOMER_SCHEMA) \
            .parquet(customer_source_path)
        return df


def read_order_data(spark, order_source_path):
    """
    Read order data from S3.

    Args:
        spark: SparkSession
        order_source_path: S3 path to order data

    Returns:
        DataFrame with order data
    """
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(ORDER_SCHEMA) \
        .csv(order_source_path)
    return df


def clean_data(df):
    """
    Clean data by removing NULL values, 'Null' strings, and duplicates.

    Args:
        df: Input DataFrame

    Returns:
        Cleaned DataFrame
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


def add_scd2_columns(df, is_new_record=True):
    """
    Add SCD Type 2 columns: IsActive, StartDate, EndDate, OpTs.

    Args:
        df: Input DataFrame
        is_new_record: Boolean indicating if these are new records

    Returns:
        DataFrame with SCD Type 2 columns
    """
    current_ts = current_timestamp()

    df_scd2 = df.withColumn("IsActive", lit(is_new_record).cast(BooleanType())) \
        .withColumn("StartDate", current_ts.cast(TimestampType())) \
        .withColumn("EndDate",
                   when(col("IsActive") == True, lit(None).cast(TimestampType()))
                   .otherwise(current_ts.cast(TimestampType()))) \
        .withColumn("OpTs", current_ts.cast(TimestampType()))

    return df_scd2


def aggregate_customer_spend(customer_df, order_df):
    """
    Aggregate customer spending from orders.

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame

    Returns:
        DataFrame with customer aggregate spend
    """
    # Calculate total spend per order
    order_with_total = order_df.withColumn(
        "TotalSpend",
        col("PricePerUnit") * col("Qty")
    )

    # Aggregate by customer
    customer_spend = order_with_total.groupBy("CustId").agg(
        _sum("TotalSpend").alias("TotalSpend"),
        count("OrderId").alias("OrderCount")
    )

    # Join with customer data
    result = customer_df.join(customer_spend, "CustId", "left") \
        .select(
            "CustId",
            "Name",
            "EmailId",
            "Region",
            coalesce(col("TotalSpend"), lit(0.0)).alias("TotalSpend"),
            coalesce(col("OrderCount"), lit(0)).alias("OrderCount")
        )

    return result


def create_order_summary(customer_df, order_df):
    """
    Create order summary by joining customer and order data.

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame

    Returns:
        DataFrame with order summary
    """
    # Join customer and order data
    order_summary = order_df.join(customer_df, "CustId", "inner") \
        .select(
            col("OrderId"),
            col("CustId"),
            col("Name").alias("CustomerName"),
            col("EmailId"),
            col("Region"),
            col("ItemName"),
            col("PricePerUnit"),
            col("Qty"),
            col("Date"),
            (col("PricePerUnit") * col("Qty")).alias("TotalAmount")
        )

    return order_summary


def write_hudi_scd2(df, output_path, table_name, record_key, precombine_field):
    """
    Write DataFrame to S3 using Hudi format with SCD Type 2 upsert.

    Args:
        df: Input DataFrame
        output_path: S3 output path
        table_name: Hudi table name
        record_key: Primary key field
        precombine_field: Field for ordering records
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


def write_parquet(df, output_path, mode="overwrite"):
    """
    Write DataFrame to S3 in Parquet format.

    Args:
        df: Input DataFrame
        output_path: S3 output path
        mode: Write mode (overwrite, append)
    """
    df.write \
        .mode(mode) \
        .parquet(output_path)


def main():
    """
    Main execution function for AWS Glue job.
    """
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'customer_source_path',
        'order_source_path',
        'curated_output_path',
        'analytics_output_path',
        'catalog_output_path'
    ])

    # Initialize Spark and Glue contexts
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Extract parameters
    customer_source_path = args['customer_source_path']
    order_source_path = args['order_source_path']
    curated_output_path = args['curated_output_path']
    analytics_output_path = args['analytics_output_path']
    catalog_output_path = args['catalog_output_path']

    # Read data
    print(f"Reading customer data from: {customer_source_path}")
    customer_df = read_customer_data(spark, customer_source_path)

    print(f"Reading order data from: {order_source_path}")
    order_df = read_order_data(spark, order_source_path)

    # Clean data
    print("Cleaning customer data...")
    customer_cleaned = clean_data(customer_df)

    print("Cleaning order data...")
    order_cleaned = clean_data(order_df)

    # Add SCD Type 2 columns to customer data
    print("Adding SCD Type 2 columns to customer data...")
    customer_scd2 = add_scd2_columns(customer_cleaned)

    # Create order summary
    print("Creating order summary...")
    order_summary = create_order_summary(customer_cleaned, order_cleaned)
    order_summary_scd2 = add_scd2_columns(order_summary)

    # Aggregate customer spend
    print("Aggregating customer spend...")
    customer_aggregate = aggregate_customer_spend(customer_cleaned, order_cleaned)
    customer_aggregate_scd2 = add_scd2_columns(customer_aggregate)

    # Write curated order summary with Hudi SCD Type 2
    print(f"Writing curated order summary to: {curated_output_path}")
    write_hudi_scd2(
        order_summary_scd2,
        curated_output_path,
        "sdlc_wizard_ordersummary",
        "OrderId",
        "OpTs"
    )

    # Write customer aggregate spend to analytics
    print(f"Writing customer aggregate spend to: {analytics_output_path}")
    write_parquet(customer_aggregate_scd2, analytics_output_path)

    # Write catalog data with Hudi SCD Type 2
    print(f"Writing catalog data to: {catalog_output_path}")
    write_hudi_scd2(
        customer_scd2,
        catalog_output_path,
        "sdlc_wizard_order",
        "CustId",
        "OpTs"
    )

    print("Job completed successfully!")

    # Commit job
    job.commit()


if __name__ == "__main__":
    main()