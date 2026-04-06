"""
AWS Glue PySpark ETL Job - Customer Order Analytics with SCD Type 2
Implements comprehensive data ingestion, cleaning, transformation, and historical tracking
"""

import sys
import yaml
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, lit, current_timestamp,
    when, trim, lower, concat_ws, sha2
)
from pyspark.sql.types import BooleanType, TimestampType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize Spark and Glue contexts (builtins for AWS Glue)
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


def get_job_parameters(config_path=None):
    """
    Load job parameters from YAML configuration file.

    Args:
        config_path (str): Path to YAML config file. If None, uses default from job args.

    Returns:
        dict: Job parameters as key-value pairs
    """
    try:
        # Try to get config path from Glue job arguments
        if config_path is None:
            try:
                args = getResolvedOptions(sys.argv, ['config_path'])
                config_path = args['config_path']
            except Exception:
                # Default config path for local testing
                config_path = 'config/glue_params.yaml'

        # Load YAML configuration
        if config_path.startswith('s3://'):
            # For S3 paths, read using boto3
            import boto3
            s3 = boto3.client('s3')
            bucket = config_path.split('/')[2]
            key = '/'.join(config_path.split('/')[3:])
            obj = s3.get_object(Bucket=bucket, Key=key)
            config_content = obj['Body'].read().decode('utf-8')
            params = yaml.safe_load(config_content)
        else:
            # For local paths
            with open(config_path, 'r') as f:
                params = yaml.safe_load(f)

        return params

    except Exception as e:
        print(f"Error loading configuration: {str(e)}")
        raise


def clean_dataframe(df, params):
    """
    Clean DataFrame by removing NULLs, 'Null' strings, and duplicates.

    Implements FR-CLEAN-001: Data Cleaning Requirements

    Args:
        df: Input DataFrame
        params: Job parameters

    Returns:
        Cleaned DataFrame
    """
    cleaned_df = df

    # Remove rows with NULL values in any column
    if params.get('remove_nulls', 'true').lower() == 'true':
        cleaned_df = cleaned_df.dropna(how='any')

    # Remove rows containing string literal "Null" in any column
    if params.get('remove_null_strings', 'true').lower() == 'true':
        null_literal = params.get('null_string_literal', 'Null')
        for column in cleaned_df.columns:
            cleaned_df = cleaned_df.filter(
                ~(trim(lower(col(column).cast("string"))) == null_literal.lower())
            )

    # Remove duplicate records
    if params.get('remove_duplicates', 'true').lower() == 'true':
        cleaned_df = cleaned_df.dropDuplicates()

    return cleaned_df


def read_customer_data(spark, params):
    """
    Read and clean customer data from S3.

    Implements FR-INGEST-001: Customer Data Ingestion

    Args:
        spark: SparkSession
        params: Job parameters

    Returns:
        Cleaned customer DataFrame
    """
    customer_path = params['customer_data_path']
    customer_format = params.get('customer_data_format', 'parquet')

    print(f"Reading customer data from: {customer_path}")
    customer_df = spark.read.format(customer_format).load(customer_path)

    # Clean customer data
    customer_df = clean_dataframe(customer_df, params)

    print(f"Customer records after cleaning: {customer_df.count()}")
    return customer_df


def read_order_data(spark, params):
    """
    Read and clean order data from S3.

    Implements FR-INGEST-001: Order Data Ingestion

    Args:
        spark: SparkSession
        params: Job parameters

    Returns:
        Cleaned order DataFrame
    """
    order_path = params['order_data_path']
    order_format = params.get('order_data_format', 'parquet')

    print(f"Reading order data from: {order_path}")
    order_df = spark.read.format(order_format).load(order_path)

    # Clean order data
    order_df = clean_dataframe(order_df, params)

    print(f"Order records after cleaning: {order_df.count()}")
    return order_df


def calculate_customer_aggregate_spend(customer_df, order_df, params):
    """
    Calculate total spend per customer by region.

    Implements FR-AGG-001: Customer Aggregation Requirements
    Implements FR-JOIN-001: Data Integration

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        params: Job parameters

    Returns:
        Aggregated spend DataFrame
    """
    join_key = params.get('join_key', 'CustId')

    # Calculate TotalAmount = PricePerUnit * Qty
    order_with_total = order_df.withColumn(
        'TotalAmount',
        col('PricePerUnit') * col('Qty')
    )

    # Join customer and order data
    joined_df = customer_df.join(
        order_with_total,
        on=join_key,
        how='inner'
    )

    # Aggregate total spend per customer by region
    aggregate_df = joined_df.groupBy(
        customer_df['CustId'],
        customer_df['Name'],
        customer_df['EmailId'],
        customer_df['Region']
    ).agg(
        _sum('TotalAmount').alias('TotalSpend')
    )

    print(f"Aggregated records: {aggregate_df.count()}")
    return aggregate_df


def write_customer_aggregate_spend(aggregate_df, params):
    """
    Write customer aggregate spend to S3 partitioned by Region.

    Args:
        aggregate_df: Aggregated DataFrame
        params: Job parameters
    """
    output_path = params['customer_aggregate_spend_path']
    output_format = params.get('aggregate_output_format', 'parquet')
    write_mode = params.get('aggregate_write_mode', 'overwrite')
    partition_column = params.get('partition_column', 'Region')

    print(f"Writing customer aggregate spend to: {output_path}")

    aggregate_df.write \
        .format(output_format) \
        .mode(write_mode) \
        .partitionBy(partition_column) \
        .save(output_path)

    print("Customer aggregate spend written successfully")


def create_order_summary_with_scd2(customer_df, order_df, params):
    """
    Create order summary with SCD Type 2 columns.

    Implements FR-SCD2-001: Historical Tracking with SCD Type 2

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        params: Job parameters

    Returns:
        Order summary DataFrame with SCD Type 2 columns
    """
    join_key = params.get('join_key', 'CustId')

    # Calculate TotalAmount
    order_with_total = order_df.withColumn(
        'TotalAmount',
        col('PricePerUnit') * col('Qty')
    )

    # Join customer and order data
    order_summary = customer_df.join(
        order_with_total,
        on=join_key,
        how='inner'
    )

    # Add SCD Type 2 columns
    current_ts = current_timestamp()

    order_summary = order_summary \
        .withColumn(params.get('scd_is_active_column', 'IsActive'), lit(True).cast(BooleanType())) \
        .withColumn(params.get('scd_start_date_column', 'StartDate'), current_ts.cast(TimestampType())) \
        .withColumn(params.get('scd_end_date_column', 'EndDate'), lit(None).cast(TimestampType())) \
        .withColumn(params.get('scd_op_ts_column', 'OpTs'), current_ts.cast(TimestampType()))

    print(f"Order summary records with SCD Type 2: {order_summary.count()}")
    return order_summary


def write_order_summary_hudi(order_summary_df, params):
    """
    Write order summary to S3 using Apache Hudi format with upsert operation.

    Implements FR-SCD2-001: Hudi-based SCD Type 2 Storage

    Args:
        order_summary_df: Order summary DataFrame
        params: Job parameters
    """
    output_path = params['order_summary_hudi_path']

    # Hudi configuration
    hudi_options = {
        'hoodie.table.name': params.get('hudi_table_name', 'order_summary_hudi'),
        'hoodie.datasource.write.recordkey.field': params.get('hudi_record_key', 'OrderId'),
        'hoodie.datasource.write.precombine.field': params.get('hudi_precombine_field', 'OpTs'),
        'hoodie.datasource.write.partitionpath.field': params.get('hudi_partition_field', 'Region'),
        'hoodie.datasource.write.operation': params.get('hudi_operation', 'upsert'),
        'hoodie.datasource.write.table.type': params.get('hudi_table_type', 'COPY_ON_WRITE'),
        'hoodie.index.type': params.get('hudi_index_type', 'BLOOM'),
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.hive_sync.enable': 'false',
        'hoodie.upsert.shuffle.parallelism': params.get('spark_shuffle_partitions', '200'),
        'hoodie.insert.shuffle.parallelism': params.get('spark_shuffle_partitions', '200')
    }

    print(f"Writing order summary to Hudi format at: {output_path}")
    print(f"Hudi configuration: {hudi_options}")

    order_summary_df.write \
        .format('hudi') \
        .options(**hudi_options) \
        .mode(params.get('hudi_write_mode', 'append')) \
        .save(output_path)

    print("Order summary written successfully to Hudi")


def register_glue_catalog_tables(spark, params):
    """
    Register tables in AWS Glue Data Catalog.

    Args:
        spark: SparkSession
        params: Job parameters
    """
    if params.get('enable_glue_datacatalog', 'true').lower() != 'true':
        return

    database_name = params.get('glue_database_name', 'sdlc_wizard_db')

    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        # Register customer aggregate spend table
        aggregate_path = params['customer_aggregate_spend_path']
        aggregate_table = params.get('aggregate_table_name', 'customer_aggregate_spend')

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{aggregate_table}
            USING parquet
            LOCATION '{aggregate_path}'
        """)

        # Register order summary Hudi table
        hudi_path = params['order_summary_hudi_path']
        hudi_table = params.get('order_summary_table_name', 'order_summary')

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{hudi_table}
            USING hudi
            LOCATION '{hudi_path}'
        """)

        print(f"Tables registered in Glue Catalog database: {database_name}")

    except Exception as e:
        print(f"Warning: Could not register tables in Glue Catalog: {str(e)}")


def main():
    """
    Main ETL execution function.

    Orchestrates the complete ETL pipeline:
    1. Load configuration
    2. Read and clean customer data (FR-INGEST-001, FR-CLEAN-001)
    3. Read and clean order data (FR-INGEST-001, FR-CLEAN-001)
    4. Calculate customer aggregate spend (FR-AGG-001, FR-JOIN-001)
    5. Write aggregate spend to S3
    6. Create order summary with SCD Type 2 (FR-SCD2-001)
    7. Write order summary to Hudi format (FR-SCD2-001)
    8. Register tables in Glue Catalog
    9. Commit job
    """
    try:
        # Load job parameters
        print("Loading job parameters...")
        params = get_job_parameters()

        # Initialize Glue job
        job_name = params.get('job_name', 'customer-order-analytics-etl')
        job.init(job_name, {})

        print(f"Starting ETL job: {job_name}")
        print(f"Timestamp: {datetime.now().isoformat()}")

        # Configure Spark settings
        spark.conf.set("spark.sql.shuffle.partitions", params.get('spark_shuffle_partitions', '200'))
        spark.conf.set("spark.sql.adaptive.enabled", params.get('spark_sql_adaptive_enabled', 'true'))

        # Step 1: Read and clean customer data
        print("\n=== Step 1: Reading Customer Data ===")
        customer_df = read_customer_data(spark, params)
        customer_df.cache()

        # Step 2: Read and clean order data
        print("\n=== Step 2: Reading Order Data ===")
        order_df = read_order_data(spark, params)
        order_df.cache()

        # Step 3: Calculate customer aggregate spend
        print("\n=== Step 3: Calculating Customer Aggregate Spend ===")
        aggregate_df = calculate_customer_aggregate_spend(customer_df, order_df, params)

        # Step 4: Write customer aggregate spend
        print("\n=== Step 4: Writing Customer Aggregate Spend ===")
        write_customer_aggregate_spend(aggregate_df, params)

        # Step 5: Create order summary with SCD Type 2
        print("\n=== Step 5: Creating Order Summary with SCD Type 2 ===")
        order_summary_df = create_order_summary_with_scd2(customer_df, order_df, params)

        # Step 6: Write order summary to Hudi
        print("\n=== Step 6: Writing Order Summary to Hudi ===")
        write_order_summary_hudi(order_summary_df, params)

        # Step 7: Register tables in Glue Catalog
        print("\n=== Step 7: Registering Tables in Glue Catalog ===")
        register_glue_catalog_tables(spark, params)

        # Commit job
        print("\n=== Job Completed Successfully ===")
        job.commit()

        print(f"End timestamp: {datetime.now().isoformat()}")

    except Exception as e:
        print(f"\n=== Job Failed ===")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        # Cleanup
        spark.catalog.clearCache()


if __name__ == "__main__":
    main()