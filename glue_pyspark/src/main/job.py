import sys
import os
from datetime import datetime
from decimal import Decimal

try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date,
    sum as spark_sum, count as spark_count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)
import yaml


REQUIRED_KEYS = [
    'customer_input_path',
    'order_input_path',
    'customer_catalog_path',
    'order_catalog_path',
    'ordersummary_hudi_path',
    'customer_aggregate_path',
    'database_name',
    'customer_table_name',
    'order_table_name',
    'ordersummary_table_name'
]


def load_yaml_defaults():
    """Load default parameters from YAML config file."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        '..',
        'config',
        'glue_params.yaml'
    )

    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('args', {})
    return {}


def get_job_parameters():
    """
    Get job parameters with dual-mode support:
    - In AWS Glue: Load YAML defaults, then override with Glue args
    - Locally: Load YAML defaults only
    """
    defaults = load_yaml_defaults()

    if GLUE_AVAILABLE and '--JOB_NAME' in sys.argv:
        keys_in_argv = [key for key in REQUIRED_KEYS if f'--{key}' in sys.argv]

        if keys_in_argv:
            glue_args = getResolvedOptions(sys.argv, keys_in_argv)
            defaults.update(glue_args)

    missing_keys = [key for key in REQUIRED_KEYS if key not in defaults]
    if missing_keys:
        raise ValueError(f"Missing required parameters: {', '.join(missing_keys)}")

    return defaults


def clean_dataframe(df):
    """
    Remove rows with NULL values or 'Null' string values.
    Remove duplicate records based on all columns.
    """
    for column in df.columns:
        df = df.filter(
            (col(column).isNotNull()) &
            (col(column) != 'Null')
        )

    df = df.dropDuplicates()

    return df


def read_customer_data(spark, input_path):
    """Read and parse customer CSV data from S3."""
    customer_schema = StructType([
        StructField("CustId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(customer_schema) \
        .load(input_path)

    return df


def read_order_data(spark, input_path):
    """Read and parse order CSV data from S3."""
    order_schema = StructType([
        StructField("OrderId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", StringType(), True),
        StructField("Qty", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("CustId", StringType(), True)
    ])

    df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(order_schema) \
        .load(input_path)

    return df


def transform_order_data(df):
    """Transform order data with proper data types."""
    df = df.withColumn("PricePerUnit", col("PricePerUnit").cast(DecimalType(10, 2))) \
           .withColumn("Qty", col("Qty").cast(IntegerType())) \
           .withColumn("Date", to_date(col("Date")))

    return df


def write_to_catalog(df, output_path, database_name, table_name, glue_context=None):
    """Write DataFrame to S3 and register in Glue Data Catalog."""
    df.write.mode("overwrite") \
        .format("parquet") \
        .save(output_path)

    if glue_context and GLUE_AVAILABLE:
        try:
            glue_context.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name
            )
        except Exception:
            pass


def get_changed_customers(spark, new_customer_df, catalog_path):
    """
    Identify customers that have changed by comparing with existing catalog.
    Returns list of CustIds that have changes.
    """
    try:
        existing_df = spark.read.format("parquet").load(catalog_path)

        new_customer_df.createOrReplaceTempView("new_customers")
        existing_df.createOrReplaceTempView("existing_customers")

        changed_query = """
        SELECT DISTINCT n.CustId
        FROM new_customers n
        LEFT JOIN existing_customers e ON n.CustId = e.CustId
        WHERE e.CustId IS NULL
           OR n.Name != e.Name
           OR n.EmailId != e.EmailId
           OR n.Region != e.Region
        """

        changed_df = spark.sql(changed_query)
        changed_cust_ids = [row.CustId for row in changed_df.collect()]

        return changed_cust_ids
    except Exception:
        return []


def prepare_ordersummary_for_hudi(order_df, changed_cust_ids):
    """
    Prepare order summary data with SCD Type 2 fields.
    Filter to only orders for changed customers if changes exist.
    """
    if changed_cust_ids:
        order_df = order_df.filter(col("CustId").isin(changed_cust_ids))

    current_ts = current_timestamp()

    ordersummary_df = order_df.withColumn("IsActive", lit(True)) \
                               .withColumn("StartDate", current_ts) \
                               .withColumn("EndDate", lit(None).cast(TimestampType())) \
                               .withColumn("OpTs", current_ts)

    return ordersummary_df


def write_hudi_table(df, hudi_path, table_name, database_name):
    """Write DataFrame to S3 using Hudi format with SCD Type 2 configuration."""
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'OrderId',
        'hoodie.datasource.write.precombine.field': 'OpTs',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.hive_style_partitioning': 'false',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': database_name,
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor'
    }

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(hudi_path)


def generate_customer_aggregate(spark, ordersummary_path, output_path):
    """
    Generate customer aggregate spend analytics.
    Aggregates from active records in order summary.
    """
    ordersummary_df = spark.read.format("hudi").load(ordersummary_path)

    active_orders = ordersummary_df.filter(col("IsActive") == True)

    active_orders = active_orders.withColumn(
        "TotalPrice",
        col("PricePerUnit") * col("Qty")
    )

    aggregate_df = active_orders.groupBy("CustId") \
        .agg(
            spark_sum("TotalPrice").alias("TotalSpend"),
            spark_count("OrderId").alias("OrderCount")
        )

    aggregate_df.write.mode("overwrite") \
        .format("parquet") \
        .save(output_path)

    return aggregate_df


def main():
    """Main execution function for the Glue job."""
    args = get_job_parameters()

    if GLUE_AVAILABLE:
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)

        if '--JOB_NAME' in sys.argv:
            job_name_args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            job.init(job_name_args['JOB_NAME'], args)
    else:
        spark = SparkSession.builder \
            .appName("CustomerOrderProcessing") \
            .getOrCreate()
        glue_context = None
        job = None

    print("Starting Customer Order Processing Pipeline")
    print(f"Parameters: {args}")

    print("Reading customer data...")
    customer_df = read_customer_data(spark, args['customer_input_path'])

    print("Cleaning customer data...")
    customer_clean_df = clean_dataframe(customer_df)

    print("Reading order data...")
    order_df = read_order_data(spark, args['order_input_path'])

    print("Cleaning order data...")
    order_clean_df = clean_dataframe(order_df)

    print("Transforming order data...")
    order_transformed_df = transform_order_data(order_clean_df)

    print("Identifying changed customers...")
    changed_cust_ids = get_changed_customers(
        spark,
        customer_clean_df,
        args['customer_catalog_path']
    )
    print(f"Found {len(changed_cust_ids)} changed customers")

    print("Writing customer data to catalog...")
    write_to_catalog(
        customer_clean_df,
        args['customer_catalog_path'],
        args['database_name'],
        args['customer_table_name'],
        glue_context
    )

    print("Writing order data to catalog...")
    write_to_catalog(
        order_transformed_df,
        args['order_catalog_path'],
        args['database_name'],
        args['order_table_name'],
        glue_context
    )

    print("Preparing order summary for Hudi...")
    ordersummary_df = prepare_ordersummary_for_hudi(
        order_transformed_df,
        changed_cust_ids
    )

    if ordersummary_df.count() > 0:
        print("Writing order summary to Hudi...")
        write_hudi_table(
            ordersummary_df,
            args['ordersummary_hudi_path'],
            args['ordersummary_table_name'],
            args['database_name']
        )

        print("Generating customer aggregate spend...")
        aggregate_df = generate_customer_aggregate(
            spark,
            args['ordersummary_hudi_path'],
            args['customer_aggregate_path']
        )

        print(f"Customer aggregate records: {aggregate_df.count()}")
    else:
        print("No order summary records to process")

    if job:
        job.commit()

    print("Pipeline completed successfully")


if __name__ == "__main__":
    main()