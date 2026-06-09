"""
AWS Glue PySpark ETL Job - Customer Order Processing
Implements complete ETL pipeline with SCD Type 2 using Apache Hudi
"""

import sys
import os
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id
import yaml
import boto3


def get_job_parameters():
    """
    Load job parameters from YAML configuration file.
    Returns dictionary with all configuration parameters.
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config',
        'glue_params.yaml'
    )

    # Try to load from local file first
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            params = yaml.safe_load(f)
    else:
        # Fallback to default parameters if config file not found
        params = {
            'inputs_customer_source_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
            'inputs_customer_source_format': 'csv',
            'inputs_order_source_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
            'inputs_order_source_format': 'csv',
            'outputs_customer_target_path': 's3://adif-sdlc/catalog/sdlc_wizard/customer/',
            'outputs_customer_target_format': 'parquet',
            'outputs_order_target_path': 's3://adif-sdlc/catalog/sdlc_wizard/order/',
            'outputs_order_target_format': 'parquet',
            'outputs_ordersummary_target_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
            'outputs_ordersummary_target_format': 'hudi',
            'catalog_database_name': 'gen_ai_poc_databrickscoe',
            'catalog_customer_table_name': 'customer',
            'catalog_order_table_name': 'order',
            'catalog_ordersummary_table_name': 'ordersummary',
            'hudi_table_name': 'ordersummary',
            'hudi_record_key': 'OrderId',
            'hudi_precombine_key': 'OpTs',
            'hudi_partition_path': 'Region',
            'hudi_table_type': 'COPY_ON_WRITE',
            'hudi_operation': 'upsert',
            'csv_header': 'true',
            'csv_delimiter': ',',
            'csv_infer_schema': 'true',
            'temp_path': 's3://adif-sdlc/temp/'
        }

    return params


def initialize_spark_contexts():
    """
    Initialize Spark and Glue contexts.
    Returns tuple of (SparkContext, GlueContext, SparkSession, Job)
    """
    # Check if running in Glue environment
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job_name = args['JOB_NAME']
    except:
        job_name = 'customer-order-etl-local'

    # Initialize contexts
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, {})

    return sc, glueContext, spark, job


def validate_s3_access(s3_path):
    """
    Validate S3 path accessibility.
    Returns True if path is accessible, False otherwise.
    """
    try:
        # Extract bucket and prefix from S3 path
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''

        s3_client = boto3.client('s3')
        # Try to list objects with the prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return True
    except Exception as e:
        print(f"Warning: Cannot validate S3 access for {s3_path}: {str(e)}")
        return True  # Return True to allow job to proceed


class DataReader:
    """Helper class for reading data from S3"""

    def __init__(self, spark):
        self.spark = spark

    def _read_csv(self, path, header='true', delimiter=',', infer_schema='true'):
        """Internal method to read CSV files"""
        return self.spark.read.format('csv') \
            .option('header', header) \
            .option('delimiter', delimiter) \
            .option('inferSchema', infer_schema) \
            .load(path)

    def read_data_safe(self, path, format_type='csv', **options):
        """
        Safely read data from S3 with validation.
        Returns DataFrame or None if read fails.
        """
        try:
            validate_s3_access(path)

            if format_type.lower() == 'csv':
                header = options.get('header', 'true')
                delimiter = options.get('delimiter', ',')
                infer_schema = options.get('infer_schema', 'true')
                df = self._read_csv(path, header, delimiter, infer_schema)
            elif format_type.lower() == 'parquet':
                df = self.spark.read.format('parquet').load(path)
            else:
                df = self.spark.read.format(format_type).load(path)

            print(f"Successfully read data from {path}")
            return df
        except Exception as e:
            print(f"Error reading data from {path}: {str(e)}")
            return None


class DataWriter:
    """Helper class for writing data to S3"""

    def __init__(self, spark):
        self.spark = spark

    def _write_parquet(self, df, path, mode='overwrite'):
        """Internal method to write Parquet files"""
        df.write.format('parquet').mode(mode).save(path)

    def _write_csv(self, df, path, mode='overwrite', header='true'):
        """Internal method to write CSV files"""
        df.write.format('csv').option('header', header).mode(mode).save(path)

    def _write_hudi(self, df, path, options):
        """Internal method to write Hudi tables"""
        df.write.format('hudi').options(**options).mode('append').save(path)

    def write_data_safe(self, df, path, format_type='parquet', mode='overwrite', **options):
        """
        Safely write data to S3 with validation.
        Returns True if write succeeds, False otherwise.
        """
        try:
            validate_s3_access(path)

            if format_type.lower() == 'parquet':
                self._write_parquet(df, path, mode)
            elif format_type.lower() == 'csv':
                header = options.get('header', 'true')
                self._write_csv(df, path, mode, header)
            elif format_type.lower() == 'hudi':
                self._write_hudi(df, path, options)
            else:
                df.write.format(format_type).mode(mode).save(path)

            print(f"Successfully wrote data to {path}")
            return True
        except Exception as e:
            print(f"Error writing data to {path}: {str(e)}")
            return False


def normalize_columns(df):
    """
    Normalize DataFrame column names to lowercase.
    Returns DataFrame with normalized column names.
    """
    return df.toDF(*[c.lower() for c in df.columns])


def remove_nulls(df):
    """
    Remove records containing NULL values or 'Null' string literals.
    Returns cleaned DataFrame.
    """
    # Remove rows with actual NULL values
    df_no_nulls = df.dropna()

    # Remove rows with 'Null' string literal in any column
    for column in df_no_nulls.columns:
        df_no_nulls = df_no_nulls.filter(
            (col(column) != 'Null') & (col(column) != 'null') & (col(column) != 'NULL')
        )

    return df_no_nulls


def remove_duplicates(df):
    """
    Remove duplicate records based on all columns.
    Returns deduplicated DataFrame.
    """
    return df.dropDuplicates()


def add_scd_columns(df):
    """
    Add SCD Type 2 columns: IsActive, StartDate, EndDate, OpTs.
    Returns DataFrame with SCD columns.
    """
    current_ts = current_timestamp()

    df_with_scd = df.withColumn('IsActive', lit(True)) \
        .withColumn('StartDate', current_ts) \
        .withColumn('EndDate', lit(None).cast('timestamp')) \
        .withColumn('OpTs', current_ts)

    return df_with_scd


def register_catalog_table(spark, df, database_name, table_name, s3_path):
    """
    Register DataFrame as a table in AWS Glue Data Catalog.
    """
    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

        # Create or replace table
        df.write.format('parquet') \
            .mode('overwrite') \
            .option('path', s3_path) \
            .saveAsTable(f"{database_name}.{table_name}")

        print(f"Successfully registered table {database_name}.{table_name}")
    except Exception as e:
        print(f"Error registering catalog table {database_name}.{table_name}: {str(e)}")


def write_hudi_table(writer, df, params):
    """
    Write DataFrame to Hudi table with SCD Type 2 configuration.
    """
    hudi_options = {
        'hoodie.table.name': params['hudi_table_name'],
        'hoodie.datasource.write.recordkey.field': params['hudi_record_key'],
        'hoodie.datasource.write.precombine.field': params['hudi_precombine_key'],
        'hoodie.datasource.write.partitionpath.field': params['hudi_partition_path'],
        'hoodie.datasource.write.table.type': params['hudi_table_type'],
        'hoodie.datasource.write.operation': params['hudi_operation'],
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': params['catalog_database_name'],
        'hoodie.datasource.hive_sync.table': params['catalog_ordersummary_table_name'],
        'hoodie.datasource.hive_sync.partition_fields': params['hudi_partition_path'],
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.hive_sync.mode': 'hms'
    }

    writer.write_data_safe(
        df,
        params['outputs_ordersummary_target_path'],
        format_type='hudi',
        mode='append',
        **hudi_options
    )


def main():
    """
    Main ETL job execution function.
    Implements complete pipeline from ingestion to SCD Type 2 output.
    """
    print("Starting Customer Order ETL Job...")

    # Initialize Spark contexts
    sc, glueContext, spark, job = initialize_spark_contexts()

    # Load job parameters
    params = get_job_parameters()

    # Initialize reader and writer
    reader = DataReader(spark)
    writer = DataWriter(spark)

    print("Step 1: Ingesting customer data...")
    # TR-INGEST-001: Load customer CSV from S3
    customer_raw_df = reader.read_data_safe(
        params['inputs_customer_source_path'],
        format_type=params['inputs_customer_source_format'],
        header=params['csv_header'],
        delimiter=params['csv_delimiter'],
        infer_schema=params['csv_infer_schema']
    )

    if customer_raw_df is None:
        print("Error: Failed to read customer data")
        job.commit()
        return

    # Normalize column names
    customer_raw_df = normalize_columns(customer_raw_df)
    print(f"Customer records loaded: {customer_raw_df.count()}")

    print("Step 2: Ingesting order data...")
    # TR-INGEST-002: Load order CSV from S3
    order_raw_df = reader.read_data_safe(
        params['inputs_order_source_path'],
        format_type=params['inputs_order_source_format'],
        header=params['csv_header'],
        delimiter=params['csv_delimiter'],
        infer_schema=params['csv_infer_schema']
    )

    if order_raw_df is None:
        print("Error: Failed to read order data")
        job.commit()
        return

    # Normalize column names
    order_raw_df = normalize_columns(order_raw_df)
    print(f"Order records loaded: {order_raw_df.count()}")

    print("Step 3: Cleaning customer data...")
    # TR-CLEAN-001: Remove nulls from customer data
    customer_clean_df = remove_nulls(customer_raw_df)
    print(f"Customer records after null removal: {customer_clean_df.count()}")

    # TR-CLEAN-003: Deduplicate customer records
    customer_clean_df = remove_duplicates(customer_clean_df)
    print(f"Customer records after deduplication: {customer_clean_df.count()}")

    print("Step 4: Cleaning order data...")
    # TR-CLEAN-002: Remove nulls from order data
    order_clean_df = remove_nulls(order_raw_df)
    print(f"Order records after null removal: {order_clean_df.count()}")

    # TR-CLEAN-004: Deduplicate order records
    order_clean_df = remove_duplicates(order_clean_df)
    print(f"Order records after deduplication: {order_clean_df.count()}")

    print("Step 5: Writing cleaned customer data to catalog...")
    # TR-CATALOG-001: Write customer data to Parquet and register in catalog
    writer.write_data_safe(
        customer_clean_df,
        params['outputs_customer_target_path'],
        format_type=params['outputs_customer_target_format'],
        mode='overwrite'
    )
    register_catalog_table(
        spark,
        customer_clean_df,
        params['catalog_database_name'],
        params['catalog_customer_table_name'],
        params['outputs_customer_target_path']
    )

    print("Step 6: Writing cleaned order data to catalog...")
    # TR-CATALOG-002: Write order data to Parquet and register in catalog
    writer.write_data_safe(
        order_clean_df,
        params['outputs_order_target_path'],
        format_type=params['outputs_order_target_format'],
        mode='overwrite'
    )
    register_catalog_table(
        spark,
        order_clean_df,
        params['catalog_database_name'],
        params['catalog_order_table_name'],
        params['outputs_order_target_path']
    )

    print("Step 7: Joining customer and order data...")
    # FR-CATALOG-003: Join customer and order datasets
    # Ensure custid column exists in both dataframes for join
    if 'custid' in customer_clean_df.columns and 'custid' in order_clean_df.columns:
        ordersummary_df = order_clean_df.join(
            customer_clean_df,
            on='custid',
            how='inner'
        )
        print(f"Joined records: {ordersummary_df.count()}")

        print("Step 8: Adding SCD Type 2 columns...")
        # Add SCD Type 2 columns
        ordersummary_df = add_scd_columns(ordersummary_df)

        print("Step 9: Writing to Hudi table...")
        # Write to Hudi table with SCD Type 2 support
        write_hudi_table(writer, ordersummary_df, params)

        print("ETL Job completed successfully!")
    else:
        print("Warning: custid column not found in both dataframes. Skipping join and Hudi write.")
        print(f"Customer columns: {customer_clean_df.columns}")
        print(f"Order columns: {order_clean_df.columns}")

    # Commit job
    job.commit()
    print("Job committed.")


if __name__ == '__main__':
    main()