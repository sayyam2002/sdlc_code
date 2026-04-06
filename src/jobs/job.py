"""
AWS Glue ETL Job - Customer Order Analytics with SCD Type 2
Implements data ingestion, cleaning, SCD Type 2 tracking, and aggregation
"""

import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, sum as _sum,
    count, avg, min as _min, max as _max, row_number
)
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def get_job_parameters():
    """
    Load job parameters from YAML configuration file.
    This function MUST be used for all parameter access.
    """
    import os
    import yaml

    base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    yaml_path = os.path.join(base, "config", "glue_params.yaml")

    with open(yaml_path) as f:
        return yaml.safe_load(f)


def initialize_glue_context():
    """Initialize Spark and Glue contexts"""
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Enable Hudi support
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

    return glue_context, spark


def read_customer_data(spark, params):
    """
    Read customer data from S3
    FR-INGEST-001: Ingest customer data
    """
    customer_path = params['inputs_customer_curated_path']
    file_format = params['inputs_customer_format']
    delimiter = params['inputs_delimiter']

    df = spark.read \
        .format(file_format) \
        .option("header", "true") \
        .option("delimiter", delimiter) \
        .option("inferSchema", "true") \
        .load(customer_path)

    return df


def read_order_data(spark, params):
    """
    Read order data from S3
    FR-INGEST-001: Ingest order data
    """
    order_path = params['inputs_order_curated_path']
    file_format = params['inputs_order_format']
    delimiter = params['inputs_delimiter']

    df = spark.read \
        .format(file_format) \
        .option("header", "true") \
        .option("delimiter", delimiter) \
        .option("inferSchema", "true") \
        .load(order_path)

    return df


def clean_data(df, params):
    """
    Apply data quality rules
    FR-CLEAN-001: Remove NULLs, 'Null' strings, and duplicates
    """
    quality_rules = params['data_quality']

    # Remove rows with NULL values
    if quality_rules['remove_nulls']:
        df = df.na.drop()

    # Remove rows with 'Null' string values
    if quality_rules['remove_null_strings']:
        null_strings = quality_rules['null_string_values']
        for column in df.columns:
            for null_value in null_strings:
                df = df.filter(col(column) != null_value)

    # Remove duplicate records
    if quality_rules['remove_duplicates']:
        df = df.dropDuplicates()

    return df


def add_scd2_columns(df, params, is_new_record=True):
    """
    Add SCD Type 2 tracking columns
    FR-SCD2-001: Implement SCD Type 2 with IsActive, StartDate, EndDate, OpTs
    """
    scd2_cols = params['scd2_columns']
    current_ts = current_timestamp()

    df = df.withColumn(scd2_cols['is_active'], lit(is_new_record)) \
           .withColumn(scd2_cols['start_date'], current_ts) \
           .withColumn(scd2_cols['end_date'], lit(None).cast("timestamp")) \
           .withColumn(scd2_cols['operation_timestamp'], current_ts)

    return df


def perform_scd2_upsert(new_df, existing_df, params):
    """
    Perform SCD Type 2 upsert logic
    FR-SCD2-001: Track historical changes
    """
    if existing_df is None:
        # First load - all records are new
        return add_scd2_columns(new_df, params, is_new_record=True)

    scd2_cols = params['scd2_columns']
    record_key = params['hudi_customer_record_key']
    current_ts = current_timestamp()

    # Identify changed records
    existing_active = existing_df.filter(col(scd2_cols['is_active']) == True)

    # Join to find changes
    joined = new_df.alias("new").join(
        existing_active.alias("old"),
        col(f"new.{record_key}") == col(f"old.{record_key}"),
        "left"
    )

    # Records that changed - expire old records
    changed_keys = joined.filter(
        (col(f"old.{record_key}").isNotNull()) &
        ((col("new.Name") != col("old.Name")) |
         (col("new.EmailId") != col("old.EmailId")) |
         (col("new.Region") != col("old.Region")))
    ).select(col(f"new.{record_key}").alias("changed_key"))

    # Expire old records
    expired_df = existing_df.join(
        changed_keys,
        existing_df[record_key] == changed_keys["changed_key"],
        "inner"
    ).withColumn(scd2_cols['is_active'], lit(False)) \
     .withColumn(scd2_cols['end_date'], current_ts) \
     .drop("changed_key")

    # Add new/changed records
    new_records = add_scd2_columns(new_df, params, is_new_record=True)

    # Union expired and new records
    result_df = expired_df.unionByName(new_records, allowMissingColumns=True)

    return result_df


def calculate_customer_aggregates(customer_df, order_df, params):
    """
    Calculate customer aggregate spend metrics
    FR-AGG-001: Aggregate customer order data
    """
    agg_config = params['aggregation']
    scd2_cols = params['scd2_columns']

    # Join customer and order data
    joined_df = order_df.join(
        customer_df.filter(col(scd2_cols['is_active']) == True),
        "CustId",
        "inner"
    )

    # Perform aggregations
    aggregate_df = joined_df.groupBy(
        *agg_config['group_by_columns']
    ).agg(
        _sum("OrderAmount").alias("TotalSpend"),
        count("OrderId").alias("OrderCount"),
        avg("OrderAmount").alias("AvgOrderValue"),
        _min("OrderDate").alias("FirstOrderDate"),
        _max("OrderDate").alias("LastOrderDate")
    )

    # Add operation timestamp
    aggregate_df = aggregate_df.withColumn(
        scd2_cols['operation_timestamp'],
        current_timestamp()
    )

    return aggregate_df


def write_hudi_table(df, params, table_type="customer"):
    """
    Write DataFrame to S3 in Hudi format
    FR-SCD2-001: Store data in Hudi format with upsert capability
    """
    if table_type == "customer":
        output_path = params['outputs_customer_scd2_path']
        table_name = params['hudi_customer_table_name']
        record_key = params['hudi_customer_record_key']
        precombine_field = params['hudi_customer_precombine_field']
        partition_field = params['hudi_customer_partition_field']
    else:  # aggregate
        output_path = params['outputs_customer_aggregate_path']
        table_name = params['hudi_aggregate_table_name']
        record_key = params['hudi_aggregate_record_key']
        precombine_field = params['hudi_aggregate_precombine_field']
        partition_field = params['hudi_aggregate_partition_field']

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.precombine.field': precombine_field,
        'hoodie.datasource.write.partitionpath.field': partition_field,
        'hoodie.datasource.write.operation': params['hudi_operation_type'],
        'hoodie.datasource.write.table.type': params['hudi_table_type'],
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': params['glue_database_name'],
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.hive_sync.partition_fields': partition_field,
        'hoodie.datasource.hive_sync.partition_extractor_class':
            'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.hive_sync.mode': 'hms'
    }

    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(output_path)


def register_glue_catalog_table(glue_context, params, table_type="customer"):
    """
    Register table in AWS Glue Data Catalog
    """
    database_name = params['glue_database_name']

    if table_type == "customer":
        table_name = params['glue_customer_table_name']
        s3_path = params['outputs_customer_scd2_path']
    else:  # aggregate
        table_name = params['glue_aggregate_table_name']
        s3_path = params['outputs_customer_aggregate_path']

    # Create database if not exists
    try:
        glue_context.create_database(database_name)
    except Exception:
        pass  # Database already exists

    # Table registration is handled by Hudi Hive sync
    print(f"Table {table_name} registered in database {database_name}")


def main():
    """
    Main ETL job execution
    Implements complete pipeline: Ingest -> Clean -> SCD2 -> Aggregate
    """
    # Initialize contexts
    glue_context, spark = initialize_glue_context()

    # Initialize Glue Job
    job = Job(glue_context)
    job.init(get_job_parameters()['job_name'], {})

    # Load parameters
    params = get_job_parameters()

    print("Starting Customer Order Analytics ETL Job with SCD Type 2")

    # FR-INGEST-001: Ingest data
    print("Step 1: Ingesting customer data...")
    customer_df = read_customer_data(spark, params)

    print("Step 2: Ingesting order data...")
    order_df = read_order_data(spark, params)

    # FR-CLEAN-001: Clean data
    print("Step 3: Cleaning customer data...")
    customer_df = clean_data(customer_df, params)

    print("Step 4: Cleaning order data...")
    order_df = clean_data(order_df, params)

    # FR-SCD2-001: Apply SCD Type 2
    print("Step 5: Applying SCD Type 2 logic...")

    # Try to read existing customer data
    try:
        existing_customer_df = spark.read \
            .format("hudi") \
            .load(params['outputs_customer_scd2_path'])
    except Exception:
        existing_customer_df = None
        print("No existing customer data found - initial load")

    customer_scd2_df = perform_scd2_upsert(
        customer_df,
        existing_customer_df,
        params
    )

    # Write customer SCD2 data
    print("Step 6: Writing customer SCD2 data to Hudi...")
    write_hudi_table(customer_scd2_df, params, table_type="customer")

    # Register in Glue Catalog
    print("Step 7: Registering customer table in Glue Catalog...")
    register_glue_catalog_table(glue_context, params, table_type="customer")

    # FR-AGG-001: Calculate aggregates
    print("Step 8: Calculating customer aggregates...")
    aggregate_df = calculate_customer_aggregates(
        customer_scd2_df,
        order_df,
        params
    )

    # Write aggregate data
    print("Step 9: Writing aggregate data to Hudi...")
    write_hudi_table(aggregate_df, params, table_type="aggregate")

    # Register in Glue Catalog
    print("Step 10: Registering aggregate table in Glue Catalog...")
    register_glue_catalog_table(glue_context, params, table_type="aggregate")

    print("ETL Job completed successfully!")

    # Commit job
    job.commit()


if __name__ == "__main__":
    main()