"""
AWS Glue PySpark Job - SDLC Wizard Customer Order Analytics
Implements SCD Type 2 using Apache Hudi with data cleaning and aggregation
"""

import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as _sum, count,
    when, trim, lower, max as _max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType
)
from awsglue.context import GlueContext
from awsglue.job import Job


def get_customer_schema():
    """Define customer data schema"""
    return StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])


def get_order_schema():
    """Define order data schema"""
    return StructType([
        StructField("OrderId", StringType(), True),
        StructField("CustId", StringType(), True),
        StructField("OrderDate", StringType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("ProductId", StringType(), True)
    ])


def clean_dataframe(df):
    """
    Clean dataframe by removing:
    1. Rows with NULL values in any column
    2. Rows with literal string 'Null' in any column (case-insensitive)
    3. Duplicate records

    Args:
        df: Input DataFrame

    Returns:
        Cleaned DataFrame
    """
    # Remove rows with NULL values
    df_no_nulls = df.dropna(how='any')

    # Remove rows with literal 'Null' string (case-insensitive)
    # Build condition to check all string columns
    null_string_condition = None
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            col_condition = (
                (lower(trim(col(field.name))) == 'null') |
                (trim(col(field.name)) == '')
            )
            if null_string_condition is None:
                null_string_condition = col_condition
            else:
                null_string_condition = null_string_condition | col_condition

    if null_string_condition is not None:
        df_cleaned = df_no_nulls.filter(~null_string_condition)
    else:
        df_cleaned = df_no_nulls

    # Remove duplicates
    df_deduped = df_cleaned.dropDuplicates()

    return df_deduped


def read_customer_data(spark, customer_source_path):
    """
    Read and clean customer data from S3

    Args:
        spark: SparkSession
        customer_source_path: S3 path to customer data

    Returns:
        Cleaned customer DataFrame
    """
    customer_schema = get_customer_schema()

    customer_df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .schema(customer_schema) \
        .csv(customer_source_path)

    return clean_dataframe(customer_df)


def read_order_data(spark, order_source_path):
    """
    Read and clean order data from S3

    Args:
        spark: SparkSession
        order_source_path: S3 path to order data

    Returns:
        Cleaned order DataFrame
    """
    order_schema = get_order_schema()

    order_df = spark.read \
        .option("header", "true") \
        .schema(order_schema) \
        .csv(order_source_path)

    return clean_dataframe(order_df)


def join_customer_order_data(customer_df, order_df):
    """
    Join customer and order data on CustId

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame

    Returns:
        Joined DataFrame
    """
    joined_df = order_df.join(
        customer_df,
        order_df.CustId == customer_df.CustId,
        "inner"
    ).select(
        order_df.OrderId,
        order_df.CustId,
        customer_df.Name,
        customer_df.EmailId,
        customer_df.Region,
        order_df.OrderDate,
        order_df.Amount,
        order_df.ProductId
    )

    return joined_df


def apply_scd_type2_columns(df):
    """
    Add SCD Type 2 columns to DataFrame

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with SCD Type 2 columns
    """
    current_ts = current_timestamp()

    scd_df = df \
        .withColumn("IsActive", lit(True).cast(BooleanType())) \
        .withColumn("StartDate", current_ts.cast(TimestampType())) \
        .withColumn("EndDate", lit(None).cast(TimestampType())) \
        .withColumn("OpTs", current_ts.cast(TimestampType()))

    return scd_df


def write_hudi_scd_type2(df, output_path, table_name, record_key):
    """
    Write DataFrame to S3 using Hudi format with SCD Type 2 upsert

    Args:
        df: DataFrame to write
        output_path: S3 output path
        table_name: Hudi table name
        record_key: Primary key field(s)
    """
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.precombine.field': 'OpTs',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.hive_style_partitioning': 'false',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }

    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(output_path)


def calculate_customer_aggregate_spend(order_summary_df):
    """
    Calculate aggregate spend per customer

    Args:
        order_summary_df: Order summary DataFrame

    Returns:
        Aggregated DataFrame with total spend per customer
    """
    aggregate_df = order_summary_df \
        .filter(col("IsActive") == True) \
        .groupBy("CustId", "Name", "EmailId", "Region") \
        .agg(
            _sum("Amount").alias("TotalSpend"),
            count("OrderId").alias("OrderCount"),
            _max("OrderDate").alias("LastOrderDate")
        ) \
        .orderBy(col("TotalSpend").desc())

    return aggregate_df


def write_aggregate_data(df, output_path):
    """
    Write aggregate data to S3 in Parquet format

    Args:
        df: DataFrame to write
        output_path: S3 output path
    """
    df.write \
        .mode("overwrite") \
        .parquet(output_path)


def register_glue_catalog_table(spark, database_name, table_name, s3_path):
    """
    Register table in AWS Glue Data Catalog

    Args:
        spark: SparkSession
        database_name: Glue database name
        table_name: Table name
        s3_path: S3 path to data
    """
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
            USING parquet
            LOCATION '{s3_path}'
        """)
    except Exception as e:
        print(f"Warning: Could not register table {table_name} in Glue Catalog: {str(e)}")


def main():
    """
    Main execution function for AWS Glue Job
    """
    # Get job parameters
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME',
            'customer_source_path',
            'order_source_path',
            'ordersummary_output_path',
            'aggregate_output_path',
            'glue_database'
        ]
    )

    # Extract parameters
    customer_source_path = args.get('customer_source_path', 's3://adif-sdlc/sdlc_wizard/customerdata/')
    order_source_path = args.get('order_source_path', 's3://adif-sdlc/sdlc_wizard/orderdata/')
    ordersummary_output_path = args.get('ordersummary_output_path', 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/')
    aggregate_output_path = args.get('aggregate_output_path', 's3://adif-sdlc/analytics/customeraggregatespend/')
    glue_database = args.get('glue_database', 'sdlc_wizard_db')

    # Initialize Spark and Glue contexts
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    print(f"Starting job: {args['JOB_NAME']}")
    print(f"Customer source: {customer_source_path}")
    print(f"Order source: {order_source_path}")
    print(f"Order summary output: {ordersummary_output_path}")
    print(f"Aggregate output: {aggregate_output_path}")
    print(f"Glue database: {glue_database}")

    # Step 1: Read and clean customer data
    print("Step 1: Reading and cleaning customer data...")
    customer_df = read_customer_data(spark, customer_source_path)
    customer_count = customer_df.count()
    print(f"Cleaned customer records: {customer_count}")

    # Step 2: Read and clean order data
    print("Step 2: Reading and cleaning order data...")
    order_df = read_order_data(spark, order_source_path)
    order_count = order_df.count()
    print(f"Cleaned order records: {order_count}")

    # Step 3: Join customer and order data
    print("Step 3: Joining customer and order data...")
    joined_df = join_customer_order_data(customer_df, order_df)
    joined_count = joined_df.count()
    print(f"Joined records: {joined_count}")

    # Step 4: Apply SCD Type 2 columns
    print("Step 4: Applying SCD Type 2 columns...")
    order_summary_df = apply_scd_type2_columns(joined_df)

    # Step 5: Write order summary with Hudi SCD Type 2
    print("Step 5: Writing order summary to Hudi format...")
    write_hudi_scd_type2(
        order_summary_df,
        ordersummary_output_path,
        'ordersummary',
        'OrderId'
    )
    print(f"Order summary written to: {ordersummary_output_path}")

    # Step 6: Calculate customer aggregate spend
    print("Step 6: Calculating customer aggregate spend...")
    aggregate_df = calculate_customer_aggregate_spend(order_summary_df)
    aggregate_count = aggregate_df.count()
    print(f"Aggregate records: {aggregate_count}")

    # Step 7: Write aggregate data
    print("Step 7: Writing aggregate data...")
    write_aggregate_data(aggregate_df, aggregate_output_path)
    print(f"Aggregate data written to: {aggregate_output_path}")

    # Step 8: Register tables in Glue Catalog
    print("Step 8: Registering tables in Glue Catalog...")
    register_glue_catalog_table(
        spark,
        glue_database,
        'ordersummary',
        ordersummary_output_path
    )
    register_glue_catalog_table(
        spark,
        glue_database,
        'customeraggregatespend',
        aggregate_output_path
    )
    print("Tables registered in Glue Catalog")

    # Commit job
    job.commit()
    print("Job completed successfully")


if __name__ == "__main__":
    main()